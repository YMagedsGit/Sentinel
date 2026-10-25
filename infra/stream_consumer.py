"""
stream_consumer.py

Kinesis-triggered Lambda: parses, validates, and lightly enriches incoming
fraud-stream records, then writes them to S3 (raw + processed) as the
event source mapping delivers batches (default batch size 100, see
LAMBDA_BATCH_SIZE in config.sh).

This is a deliberately minimal placeholder so deploy.sh has something real
to deploy — the parse/validate/enrich logic is marked with TODOs. Wire in
your actual feature-enrichment rules there.
"""

import base64
import json
import os
import time
import uuid

import boto3

s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]


def _decode_record(kinesis_record: dict) -> dict:
    payload = base64.b64decode(kinesis_record["kinesis"]["data"])
    return json.loads(payload)


def _validate(record: dict) -> bool:
    # TODO: replace with real schema validation against model_meta['features']
    return isinstance(record, dict) and len(record) > 0


def _enrich(record: dict) -> dict:
    # TODO: replace with real enrichment (e.g. derived ratios, lookups)
    record["_ingested_at"] = time.time()
    return record


def _write_batch(bucket: str, records: list[dict]) -> None:
    if not records:
        return
    key = f"batch-{int(time.time())}-{uuid.uuid4().hex[:8]}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def handler(event, context):
    raw_records = []
    processed_records = []
    skipped = 0

    for kinesis_record in event.get("Records", []):
        record = _decode_record(kinesis_record)
        raw_records.append(record)

        if not _validate(record):
            skipped += 1
            continue

        processed_records.append(_enrich(record))

    _write_batch(RAW_BUCKET, raw_records)
    _write_batch(PROCESSED_BUCKET, processed_records)

    return {
        "received": len(raw_records),
        "processed": len(processed_records),
        "skipped": skipped,
    }
