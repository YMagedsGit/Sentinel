#!/usr/bin/env bash
# config.sh — shared variables for deploy.sh / cleanup.sh
# Source this file; do not execute directly.

set -euo pipefail

export AWS_REGION="${AWS_REGION:-eu-north-1}"
export PROJECT="${PROJECT:-fraud-drift-demo}"

# Resolve account ID once; used to make bucket names globally unique.
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text --region "$AWS_REGION")"
export ACCOUNT_ID

export RAW_BUCKET="${PROJECT}-raw-${ACCOUNT_ID}"
export PROCESSED_BUCKET="${PROJECT}-processed-${ACCOUNT_ID}"

export STREAM_NAME="${PROJECT}-stream"
export LAMBDA_NAME="${PROJECT}-stream-consumer"
export LAMBDA_ROLE_NAME="${PROJECT}-lambda-role"
export LAMBDA_POLICY_NAME="${PROJECT}-lambda-s3-policy"

# Starting position for the Kinesis->Lambda event source mapping.
# TRIM_HORIZON: consume everything currently in the stream (useful while
# testing with producer.py replaying from the start).
# LATEST: only consume records arriving after the mapping is created.
export ESM_STARTING_POSITION="${ESM_STARTING_POSITION:-TRIM_HORIZON}"
export LAMBDA_BATCH_SIZE="${LAMBDA_BATCH_SIZE:-100}"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
warn() { echo "[$(date '+%H:%M:%S')] WARNING: $*" >&2; }
die()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed."
}
