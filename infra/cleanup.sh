#!/usr/bin/env bash
# cleanup.sh — cost-focused teardown.
#
# Default scope (matches what was asked for):
#   - delete the Kinesis stream
#   - stop any EMR Serverless applications whose name starts with $PROJECT
#   - delete any SageMaker endpoints whose name starts with $PROJECT
#
# EMR Serverless and SageMaker resources are NOT created by deploy.sh (they
# come later in the pipeline) — this script finds them by name prefix, so
# it only touches resources tagged/named under the current $PROJECT.
#
# --all additionally removes the Lambda, event source mapping, IAM role, and
# S3 buckets (destructive — empties and deletes bucket contents). Requires
# typing DELETE to confirm, unless run with --yes.
#
# Usage:
#   ./cleanup.sh            # stream + EMR stop + SageMaker endpoint delete
#   ./cleanup.sh --all      # also removes Lambda/IAM/S3 (destructive)
#   ./cleanup.sh --all --yes   # skip the typed confirmation (for CI)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_cmd aws

FULL_TEARDOWN=false
SKIP_CONFIRM=false
for arg in "$@"; do
    case "$arg" in
        --all) FULL_TEARDOWN=true ;;
        --yes) SKIP_CONFIRM=true ;;
        *) die "Unknown argument: $arg (expected --all and/or --yes)" ;;
    esac
done

log "Project: $PROJECT | Region: $AWS_REGION | Account: $ACCOUNT_ID"
if $FULL_TEARDOWN; then
    warn "Full teardown requested: this also deletes Lambda, IAM role, and S3 bucket CONTENTS."
    if ! $SKIP_CONFIRM; then
        read -r -p "Type DELETE to confirm full teardown: " CONFIRM
        [[ "$CONFIRM" == "DELETE" ]] || die "Confirmation did not match. Aborting."
    fi
fi

# --------------------------------------------------------------------------- #
# 1. Kinesis stream
# --------------------------------------------------------------------------- #

if aws kinesis describe-stream-summary --stream-name "$STREAM_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log "Deleting Kinesis stream: $STREAM_NAME"
    aws kinesis delete-stream --stream-name "$STREAM_NAME" --region "$AWS_REGION"
    aws kinesis wait stream-not-exists --stream-name "$STREAM_NAME" --region "$AWS_REGION" || true
else
    log "Kinesis stream not found, skipping: $STREAM_NAME"
fi

# --------------------------------------------------------------------------- #
# 2. EMR Serverless — stop applications (does not delete them)
# --------------------------------------------------------------------------- #

EMR_APP_IDS="$(aws emr-serverless list-applications \
    --region "$AWS_REGION" \
    --query "applications[?starts_with(name, \`${PROJECT}\`)].id" \
    --output text 2>/dev/null || true)"

if [[ -z "$EMR_APP_IDS" ]]; then
    log "No EMR Serverless applications found with prefix: $PROJECT"
else
    for app_id in $EMR_APP_IDS; do
        STATE="$(aws emr-serverless get-application --application-id "$app_id" --region "$AWS_REGION" \
            --query 'application.state' --output text)"
        log "EMR Serverless app $app_id is $STATE"

        if [[ "$STATE" == "STARTED" || "$STATE" == "STARTING" ]]; then
            RUNNING_JOBS="$(aws emr-serverless list-job-runs \
                --application-id "$app_id" --states RUNNING SCHEDULED PENDING \
                --region "$AWS_REGION" --query 'jobRuns[].id' --output text)"
            for job_id in $RUNNING_JOBS; do
                log "Cancelling running job $job_id on app $app_id"
                aws emr-serverless cancel-job-run \
                    --application-id "$app_id" --job-run-id "$job_id" --region "$AWS_REGION" >/dev/null
            done
            log "Stopping EMR Serverless app: $app_id"
            aws emr-serverless stop-application --application-id "$app_id" --region "$AWS_REGION"
        else
            log "EMR Serverless app $app_id already stopped, skipping"
        fi

        if $FULL_TEARDOWN; then
            log "Deleting EMR Serverless app: $app_id"
            aws emr-serverless delete-application --application-id "$app_id" --region "$AWS_REGION" || true
        fi
    done
fi

# --------------------------------------------------------------------------- #
# 3. SageMaker — delete endpoints (and configs, under --all)
# --------------------------------------------------------------------------- #

SM_ENDPOINTS="$(aws sagemaker list-endpoints \
    --region "$AWS_REGION" \
    --name-contains "$PROJECT" \
    --query 'Endpoints[].EndpointName' --output text 2>/dev/null || true)"

if [[ -z "$SM_ENDPOINTS" ]]; then
    log "No SageMaker endpoints found matching: $PROJECT"
else
    for endpoint in $SM_ENDPOINTS; do
        log "Deleting SageMaker endpoint: $endpoint"
        aws sagemaker delete-endpoint --endpoint-name "$endpoint" --region "$AWS_REGION" || true

        if $FULL_TEARDOWN; then
            CONFIG_NAME="$(aws sagemaker describe-endpoint --endpoint-name "$endpoint" --region "$AWS_REGION" \
                --query 'EndpointConfigName' --output text 2>/dev/null || true)"
            if [[ -n "$CONFIG_NAME" && "$CONFIG_NAME" != "None" ]]; then
                log "Deleting SageMaker endpoint config: $CONFIG_NAME"
                aws sagemaker delete-endpoint-config --endpoint-config-name "$CONFIG_NAME" --region "$AWS_REGION" || true
            fi
        fi
    done
fi

if ! $FULL_TEARDOWN; then
    cat <<EOF

Cleanup complete (default scope: Kinesis stream deleted, EMR Serverless stopped, SageMaker endpoints deleted).
Lambda, IAM role, and S3 buckets were left in place. Re-run with --all to remove those too.
EOF
    exit 0
fi

# --------------------------------------------------------------------------- #
# 4. (--all only) Lambda + event source mapping + IAM role + S3 buckets
# --------------------------------------------------------------------------- #

ESM_UUIDS="$(aws lambda list-event-source-mappings \
    --function-name "$LAMBDA_NAME" --region "$AWS_REGION" \
    --query 'EventSourceMappings[].UUID' --output text 2>/dev/null || true)"
for uuid in $ESM_UUIDS; do
    log "Deleting event source mapping: $uuid"
    aws lambda delete-event-source-mapping --uuid "$uuid" --region "$AWS_REGION" || true
done

if aws lambda get-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log "Deleting Lambda: $LAMBDA_NAME"
    aws lambda delete-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION"
fi

if aws iam get-role --role-name "$LAMBDA_ROLE_NAME" >/dev/null 2>&1; then
    log "Detaching managed policies and deleting IAM role: $LAMBDA_ROLE_NAME"
    aws iam detach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 2>/dev/null || true
    aws iam detach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole 2>/dev/null || true
    aws iam delete-role-policy --role-name "$LAMBDA_ROLE_NAME" \
        --policy-name "$LAMBDA_POLICY_NAME" 2>/dev/null || true
    aws iam delete-role --role-name "$LAMBDA_ROLE_NAME"
fi

empty_and_delete_bucket() {
    local bucket="$1"
    if aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
        log "Emptying and deleting S3 bucket: $bucket"
        aws s3 rm "s3://$bucket" --recursive --region "$AWS_REGION" >/dev/null
        aws s3api delete-bucket --bucket "$bucket" --region "$AWS_REGION"
    else
        log "S3 bucket not found, skipping: $bucket"
    fi
}
empty_and_delete_bucket "$RAW_BUCKET"
empty_and_delete_bucket "$PROCESSED_BUCKET"

cat <<EOF

Full teardown complete: Kinesis, EMR Serverless (stopped/deleted), SageMaker endpoints,
Lambda, event source mapping, IAM role, and S3 buckets have all been removed.
EOF
