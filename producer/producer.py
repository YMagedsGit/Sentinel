"""
producer.py

Streams rows from a parquet file to a Kinesis Data Stream in batches of 100,
injecting synthetic feature drift at configurable record-count checkpoints
(default: step 500 and step 1000).

Assumes (matches baseline_model_meta.json / baseline_stats.json / unused_dataset.parquet):
    - model_meta.json ("baseline_model_meta.json") contains {"features": [...], ...}
      — no numeric_features key, so numeric drift candidates are inferred from
      the parquet's own dtypes.
    - stats.json ("baseline_stats.json") holds AGGREGATE metrics only
      (roc_auc_score, train/test sample counts, fraud percentages) — it has
      no per-feature mean/std, so it is NOT used for drift scaling. Instead,
      per-feature std is computed directly from the parquet data being
      streamed (the actual distribution you're injecting drift against).
    - The parquet file ("unused_dataset.parquet") contains model_meta["features"]
      plus a label column (default "isFraud"), which is passed through
      untouched and excluded from drift.
    - Record order in the parquet is NOT a clean 0..N index (it's leftover
      rows from a train/test split), so drift staging uses a sequential
      position counter, not df.index. The original index is preserved per
      record as "_row_id" for traceability.

baseline_model.pkl is NOT used here (producer only emits raw/drifted feature
rows). If you need the model to pre-score rows before sending, say so and
I'll add it.

Usage:
    python producer.py \
        --parquet unused_dataset.parquet \
        --model-meta baseline_model_meta.json \
        --stats baseline_stats.json \
        --stream-name my-kinesis-stream \
        --region us-east-1 \
        --batch-size 100 \
        --drift-steps 500 1000 \
        --drift-magnitudes 1.5 3.0 \
        --label-col isFraud
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import boto3
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("producer")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def json_default(obj: Any):
    """Handle numpy / pandas types that json.dumps chokes on."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def resolve_drift_stage(record_index: int, drift_steps: list[int]) -> int:
    """
    Returns the drift stage (0 = no drift, 1 = first threshold crossed, ...)
    for a given cumulative record_index, based on sorted drift_steps.
    """
    stage = 0
    for step in sorted(drift_steps):
        if record_index >= step:
            stage += 1
    return stage


def apply_drift(
    row: dict,
    stage: int,
    drift_features: list[str],
    drift_magnitudes: list[float],
    feature_stds: dict,
) -> dict:
    """
    Shifts drift_features by (magnitude * std) for the given stage, where
    feature_stds is computed from the reference data itself (see main()).
    stage 0 = untouched. stage 1 uses drift_magnitudes[0], stage 2 uses
    drift_magnitudes[1], etc. If stage exceeds available magnitudes, the
    last magnitude is reused.
    """
    if stage == 0:
        return row

    magnitude = drift_magnitudes[min(stage - 1, len(drift_magnitudes) - 1)]
    drifted = dict(row)

    for feat in drift_features:
        std = feature_stds.get(feat)
        if not std or drifted.get(feat) is None:
            continue
        drifted[feat] = drifted[feat] + magnitude * std

    drifted["_drift_stage"] = stage
    drifted["_drift_magnitude"] = magnitude
    return drifted


def chunked(iterable, size):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


# --------------------------------------------------------------------------- #
# Kinesis
# --------------------------------------------------------------------------- #

def put_batch_with_retry(
    client,
    stream_name: str,
    records: list[dict],
    max_retries: int = 5,
) -> None:
    """
    Sends a batch via put_records, retrying only the records that failed.
    Kinesis put_records can partially fail without raising an exception.
    """
    entries = [
        {
            "Data": json.dumps(r["data"], default=json_default).encode("utf-8"),
            "PartitionKey": r["partition_key"],
        }
        for r in records
    ]

    attempt = 0
    while entries and attempt <= max_retries:
        response = client.put_records(StreamName=stream_name, Records=entries)
        failed_count = response.get("FailedRecordCount", 0)

        if failed_count == 0:
            return

        logger.warning(
            "Batch had %d failed records (attempt %d/%d). Retrying those only.",
            failed_count, attempt + 1, max_retries,
        )

        retry_entries = []
        for entry, result in zip(entries, response["Records"]):
            if "ErrorCode" in result:
                retry_entries.append(entry)
        entries = retry_entries
        attempt += 1
        time.sleep(min(2 ** attempt, 10))  # exponential backoff, capped

    if entries:
        raise RuntimeError(
            f"{len(entries)} records permanently failed after {max_retries} retries."
        )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Stream parquet rows to Kinesis with staged drift injection.")
    parser.add_argument("--parquet", required=True, help="Path to input parquet file")
    parser.add_argument("--model-meta", required=True, help="Path to model_meta.json")
    parser.add_argument("--stats", required=True,
                         help="Path to baseline_stats.json (aggregate metrics, logged for context only — "
                              "NOT used for drift scaling)")
    parser.add_argument("--label-col", default="isFraud",
                         help="Label column present in the parquet but not in model_meta['features']; "
                              "passed through untouched and excluded from drift")
    parser.add_argument("--stream-name", required=True, help="Kinesis stream name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--batch-size", type=int, default=100, help="Records per put_records call")
    parser.add_argument("--partition-key-field", default=None,
                         help="Column to use as Kinesis partition key; defaults to the record's "
                              "original parquet index (_row_id)")
    parser.add_argument("--drift-steps", type=int, nargs="+", default=[500, 1000],
                         help="Cumulative record indices at which drift severity increases")
    parser.add_argument("--drift-magnitudes", type=float, nargs="+", default=[1.5, 3.0],
                         help="Std-dev multipliers applied per drift stage, aligned with --drift-steps")
    parser.add_argument("--drift-features", nargs="+", default=None,
                         help="Feature columns to drift; defaults to all numeric-dtype columns among "
                              "model_meta['features'] (inferred from the parquet, since model_meta "
                              "has no numeric_features key)")
    parser.add_argument("--throttle-seconds", type=float, default=0.0,
                         help="Optional sleep between batches to simulate real-time streaming")
    parser.add_argument("--dry-run", action="store_true",
                         help="Build batches and log them without calling Kinesis")
    args = parser.parse_args()

    if len(args.drift_magnitudes) < len(args.drift_steps):
        logger.error("Need at least as many --drift-magnitudes as --drift-steps.")
        sys.exit(1)

    model_meta = load_json(args.model_meta)
    baseline_stats = load_json(args.stats)  # aggregate metrics only — context/logging, not drift scaling

    features = model_meta.get("features")
    if not features:
        logger.error("model_meta.json must contain a 'features' list.")
        sys.exit(1)

    logger.info(
        "Baseline context — roc_auc: %s, train_samples: %s, fraud_pct_train: %s",
        baseline_stats.get("roc_auc_score"),
        baseline_stats.get("train_samples"),
        baseline_stats.get("fraud_percentage_train"),
    )

    df = pd.read_parquet(args.parquet)

    missing = [f for f in features if f not in df.columns]
    if missing:
        logger.error("Parquet file is missing expected feature columns: %s", missing)
        sys.exit(1)

    has_label = args.label_col in df.columns
    if not has_label:
        logger.warning("Label column '%s' not found in parquet — proceeding without it.", args.label_col)

    keep_cols = features + ([args.label_col] if has_label else [])
    df = df[keep_cols].reset_index(drop=False).rename(columns={"index": "_row_id"})
    logger.info("Loaded %d rows, %d feature columns from %s", len(df), len(features), args.parquet)

    # Numeric drift candidates: explicit --drift-features, else infer from dtypes
    # among model_meta['features'] (model_meta has no numeric_features key).
    if args.drift_features:
        drift_features = args.drift_features
    else:
        numeric_dtypes = df[features].select_dtypes(include="number").columns.tolist()
        drift_features = numeric_dtypes

    if not drift_features:
        logger.warning("No drift_features resolved — drift will have no effect.")
    else:
        logger.info("Drift will be applied to %d feature(s): %s", len(drift_features), drift_features)

    # Per-feature std computed from the actual data being streamed (the reference
    # distribution), since baseline_stats.json has no per-feature stats.
    feature_stds = df[drift_features].std(numeric_only=True).to_dict()

    client = None
    if not args.dry_run:
        client = boto3.client("kinesis", region_name=args.region)

    total_sent = 0
    total_drifted = 0

    def record_generator():
        # Sequential position, NOT df['_row_id'] — the parquet's original index is
        # non-contiguous (it's the left-out split), so drift steps must key off
        # record count, not index label.
        for position, row in enumerate(df.to_dict(orient="records")):
            row_dict = dict(row)
            stage = resolve_drift_stage(position, args.drift_steps)
            if stage > 0:
                row_dict = apply_drift(
                    row_dict, stage, drift_features, args.drift_magnitudes, feature_stds
                )
            partition_key = (
                str(row_dict[args.partition_key_field])
                if args.partition_key_field and args.partition_key_field in row_dict
                else str(row_dict["_row_id"])
            )
            yield {"data": row_dict, "partition_key": partition_key, "stage": stage}

    for batch_num, batch in enumerate(chunked(record_generator(), args.batch_size), start=1):
        drifted_in_batch = sum(1 for r in batch if r["stage"] > 0)
        total_drifted += drifted_in_batch
        total_sent += len(batch)

        if args.dry_run:
            logger.info(
                "[DRY RUN] Batch %d: %d records (%d drifted). Sample: %s",
                batch_num, len(batch), drifted_in_batch,
                json.dumps(batch[0]["data"], default=json_default)[:300],
            )
        else:
            put_batch_with_retry(client, args.stream_name, batch)
            logger.info(
                "Sent batch %d: %d records (%d drifted). Total sent: %d",
                batch_num, len(batch), drifted_in_batch, total_sent,
            )

        if args.throttle_seconds:
            time.sleep(args.throttle_seconds)

    logger.info(
        "Done. Total records sent: %d, total drifted: %d, stream: %s",
        total_sent, total_drifted, args.stream_name,
    )


if __name__ == "__main__":
    main()
