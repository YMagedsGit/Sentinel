#!/usr/bin/env bash
# deploy.sh — provisions S3 (raw + processed), a Kinesis stream, an IAM role,
# and a Kinesis-triggered Lambda ("stream-consumer").
#
# Safe to re-run: every step checks whether the resource already exists
# before creating it.
#
# Usage:
#   ./deploy.sh
#
# Override defaults via env vars, e.g.:
#   PROJECT=my-fraud-demo AWS_REGION=eu-north-1 ./deploy.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_cmd aws
require_cmd zip

log "Project: $PROJECT | Region: $AWS_REGION | Account: $ACCOUNT_ID"

# --------------------------------------------------------------------------- #
# 1. S3 buckets (raw + processed)
# --------------------------------------------------------------------------- #

create_bucket_if_missing() {
    local bucket="$1"
    if aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
        log "S3 bucket already exists: $bucket"
        return
    fi
    log "Creating S3 bucket: $bucket"
    aws s3api create-bucket \
        --bucket "$bucket" \
        --region "$AWS_REGION" \
        --create-bucket-configuration LocationConstraint="$AWS_REGION" \
        >/dev/null

    aws s3api put-public-access-block \
        --bucket "$bucket" \
        --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

    aws s3api put-bucket-encryption \
        --bucket "$bucket" \
        --server-side-encryption-configuration \
        '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
}

create_bucket_if_missing "$RAW_BUCKET"
create_bucket_if_missing "$PROCESSED_BUCKET"

# --------------------------------------------------------------------------- #
# 2. Kinesis stream (on-demand, 24h retention)
# --------------------------------------------------------------------------- #

if aws kinesis describe-stream-summary --stream-name "$STREAM_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log "Kinesis stream already exists: $STREAM_NAME"
else
    log "Creating Kinesis stream: $STREAM_NAME (on-demand)"
    aws kinesis create-stream \
        --stream-name "$STREAM_NAME" \
        --stream-mode-details StreamMode=ON_DEMAND \
        --region "$AWS_REGION"

    log "Waiting for stream to become ACTIVE..."
    aws kinesis wait stream-exists --stream-name "$STREAM_NAME" --region "$AWS_REGION"
fi

# 24h is already the AWS default; call is idempotent-safe (ignore "already at this value").
aws kinesis increase-stream-retention-period \
    --stream-name "$STREAM_NAME" \
    --retention-period-hours 24 \
    --region "$AWS_REGION" 2>/dev/null || true

STREAM_ARN="$(aws kinesis describe-stream-summary \
    --stream-name "$STREAM_NAME" --region "$AWS_REGION" \
    --query 'StreamDescriptionSummary.StreamARN' --output text)"
log "Stream ARN: $STREAM_ARN"

# --------------------------------------------------------------------------- #
# 3. IAM role for the Lambda
# --------------------------------------------------------------------------- #

TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

if aws iam get-role --role-name "$LAMBDA_ROLE_NAME" >/dev/null 2>&1; then
    log "IAM role already exists: $LAMBDA_ROLE_NAME"
else
    log "Creating IAM role: $LAMBDA_ROLE_NAME"
    aws iam create-role \
        --role-name "$LAMBDA_ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        >/dev/null
fi

ROLE_ARN="$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query 'Role.Arn' --output text)"

# Managed policies: CloudWatch Logs + Kinesis read access (GetRecords, GetShardIterator,
# DescribeStream, ListStreams — everything the event source mapping needs).
aws iam attach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null
aws iam attach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole >/dev/null

# Inline policy: scoped S3 write access to just the two buckets this Lambda writes to.
S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject"],
    "Resource": [
      "arn:aws:s3:::${RAW_BUCKET}/*",
      "arn:aws:s3:::${PROCESSED_BUCKET}/*"
    ]
  }]
}
EOF
)
aws iam put-role-policy \
    --role-name "$LAMBDA_ROLE_NAME" \
    --policy-name "$LAMBDA_POLICY_NAME" \
    --policy-document "$S3_POLICY"

log "Waiting for IAM role propagation..."
sleep 10

# --------------------------------------------------------------------------- #
# 4. Lambda function (stream-consumer)
# --------------------------------------------------------------------------- #

BUILD_DIR="$(mktemp -d)"
cp "$SCRIPT_DIR/lambda/stream_consumer.py" "$BUILD_DIR/"
ZIP_PATH="$BUILD_DIR/stream_consumer.zip"
(cd "$BUILD_DIR" && zip -q "$ZIP_PATH" stream_consumer.py)

if aws lambda get-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log "Lambda exists, updating code + config: $LAMBDA_NAME"
    aws lambda update-function-code \
        --function-name "$LAMBDA_NAME" \
        --zip-file "fileb://$ZIP_PATH" \
        --region "$AWS_REGION" >/dev/null
    aws lambda wait function-updated --function-name "$LAMBDA_NAME" --region "$AWS_REGION"
    aws lambda update-function-configuration \
        --function-name "$LAMBDA_NAME" \
        --environment "Variables={RAW_BUCKET=$RAW_BUCKET,PROCESSED_BUCKET=$PROCESSED_BUCKET}" \
        --region "$AWS_REGION" >/dev/null
else
    log "Creating Lambda: $LAMBDA_NAME"
    aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime python3.12 \
        --role "$ROLE_ARN" \
        --handler stream_consumer.handler \
        --zip-file "fileb://$ZIP_PATH" \
        --timeout 60 \
        --memory-size 256 \
        --environment "Variables={RAW_BUCKET=$RAW_BUCKET,PROCESSED_BUCKET=$PROCESSED_BUCKET}" \
        --region "$AWS_REGION" >/dev/null
fi

aws lambda wait function-active --function-name "$LAMBDA_NAME" --region "$AWS_REGION"
LAMBDA_ARN="$(aws lambda get-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION" \
    --query 'Configuration.FunctionArn' --output text)"
log "Lambda ARN: $LAMBDA_ARN"
rm -rf "$BUILD_DIR"

# --------------------------------------------------------------------------- #
# 5. Event source mapping: Kinesis -> Lambda
# --------------------------------------------------------------------------- #

EXISTING_ESM="$(aws lambda list-event-source-mappings \
    --function-name "$LAMBDA_NAME" \
    --event-source-arn "$STREAM_ARN" \
    --region "$AWS_REGION" \
    --query 'EventSourceMappings[0].UUID' --output text 2>/dev/null || echo "None")"

if [[ "$EXISTING_ESM" != "None" && -n "$EXISTING_ESM" ]]; then
    log "Event source mapping already exists: $EXISTING_ESM"
else
    log "Creating event source mapping (batch size $LAMBDA_BATCH_SIZE, starting position $ESM_STARTING_POSITION)"
    aws lambda create-event-source-mapping \
        --function-name "$LAMBDA_NAME" \
        --event-source-arn "$STREAM_ARN" \
        --batch-size "$LAMBDA_BATCH_SIZE" \
        --starting-position "$ESM_STARTING_POSITION" \
        --region "$AWS_REGION" >/dev/null
fi

# --------------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------------- #

cat <<EOF

Deployment complete.
  Region:            $AWS_REGION
  S3 raw bucket:      $RAW_BUCKET
  S3 processed bucket: $PROCESSED_BUCKET
  Kinesis stream:     $STREAM_NAME
  IAM role:           $LAMBDA_ROLE_NAME
  Lambda:              $LAMBDA_NAME

Point producer.py at this stream with:
  --stream-name $STREAM_NAME --region $AWS_REGION
EOF
