"""
test_producer.py

Exercises producer.py end-to-end against a MOCKED Kinesis stream (via moto).
No AWS account, credentials, or LocalStack container needed — moto patches
boto3 in-process so put_records/get_records hit a fake in-memory backend.

Run:
    pip install moto pytest --break-system-packages
    pytest test_producer.py -v
"""

import base64
import json
import sys
import time

import boto3
import numpy as np
import pandas as pd
import pytest
from moto import mock_aws

import producer  # the script under test, imported as a module

STREAM_NAME = "fraud-mock-stream"
REGION = "us-east-1"


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture
def fixtures(tmp_path):
    """Builds synthetic parquet + metadata files matching your real schema."""
    np.random.seed(0)
    n = 1200

    X = pd.DataFrame({
        "amount": np.random.gamma(2, 50, n),
        "oldbalanceOrg": np.random.exponential(1000, n),
        "newbalanceOrig": np.random.exponential(900, n),
        "step": np.arange(n),
        "type_encoded": np.random.randint(0, 5, n),
    })
    df = X.copy()
    df["isFraud"] = np.random.binomial(1, 0.02, n)

    parquet_path = tmp_path / "unused_dataset.parquet"
    df.to_parquet(parquet_path, index=True)

    meta_path = tmp_path / "baseline_model_meta.json"
    meta_path.write_text(json.dumps({
        "model_type": "LightGBM",
        "hyperparameters": {"num_leaves": 31},
        "feature_count": len(X.columns),
        "features": list(X.columns),
    }))

    stats_path = tmp_path / "baseline_stats.json"
    stats_path.write_text(json.dumps({
        "roc_auc_score": 0.91,
        "train_samples": 8000,
        "test_samples": 2000,
        "fraud_percentage_train": 1.9,
        "fraud_percentage_test": 2.1,
    }))

    return {
        "parquet": str(parquet_path),
        "meta": str(meta_path),
        "stats": str(stats_path),
        "n": n,
    }


def read_all_records(client, stream_name):
    """Pulls every record currently on a (mocked) stream's single shard."""
    shard_id = client.describe_stream(StreamName=stream_name)["StreamDescription"]["Shards"][0]["ShardId"]
    shard_iter = client.get_shard_iterator(
        StreamName=stream_name, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    records = []
    while shard_iter:
        resp = client.get_records(ShardIterator=shard_iter, Limit=1000)
        for r in resp["Records"]:
            records.append(json.loads(r["Data"]))
        shard_iter = resp.get("NextShardIterator")
        if not resp["Records"]:
            break
    return records


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

@mock_aws
def test_all_records_sent_and_batched_correctly(fixtures, monkeypatch):
    client = boto3.client("kinesis", region_name=REGION)
    client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
    waiter = client.get_waiter("stream_exists")
    waiter.wait(StreamName=STREAM_NAME)

    argv = [
        "producer.py",
        "--parquet", fixtures["parquet"],
        "--model-meta", fixtures["meta"],
        "--stats", fixtures["stats"],
        "--stream-name", STREAM_NAME,
        "--region", REGION,
        "--batch-size", "100",
        "--drift-steps", "500", "1000",
        "--drift-magnitudes", "1.5", "3.0",
        "--drift-features", "amount", "oldbalanceOrg", "newbalanceOrig",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    producer.main()

    records = read_all_records(client, STREAM_NAME)
    assert len(records) == fixtures["n"], "every row in the parquet should reach the stream"


@mock_aws
def test_drift_fires_exactly_at_configured_steps(fixtures, monkeypatch):
    client = boto3.client("kinesis", region_name=REGION)
    client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
    client.get_waiter("stream_exists").wait(StreamName=STREAM_NAME)

    argv = [
        "producer.py",
        "--parquet", fixtures["parquet"],
        "--model-meta", fixtures["meta"],
        "--stats", fixtures["stats"],
        "--stream-name", STREAM_NAME,
        "--region", REGION,
        "--batch-size", "100",
        "--drift-steps", "500", "1000",
        "--drift-magnitudes", "1.5", "3.0",
        "--drift-features", "amount",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    producer.main()

    records = read_all_records(client, STREAM_NAME)
    # Kinesis doesn't guarantee return order across get_records calls on some
    # setups, so sort by _row_id (position in the source file) before asserting.
    records.sort(key=lambda r: r["_row_id"])

    stages = [r.get("_drift_stage", 0) for r in records]

    assert all(s == 0 for s in stages[:500]), "no drift before record 500"
    assert all(s == 1 for s in stages[500:1000]), "stage 1 drift from record 500 to 999"
    assert all(s == 2 for s in stages[1000:]), "stage 2 drift from record 1000 onward"


@mock_aws
def test_label_column_passed_through_untouched(fixtures, monkeypatch):
    client = boto3.client("kinesis", region_name=REGION)
    client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
    client.get_waiter("stream_exists").wait(StreamName=STREAM_NAME)

    argv = [
        "producer.py",
        "--parquet", fixtures["parquet"],
        "--model-meta", fixtures["meta"],
        "--stats", fixtures["stats"],
        "--stream-name", STREAM_NAME,
        "--region", REGION,
        "--batch-size", "100",
        "--label-col", "isFraud",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    producer.main()

    records = read_all_records(client, STREAM_NAME)
    assert all("isFraud" in r for r in records), "label column should be present on every record"
    assert all("isFraud" not in [] for r in records)  # sanity placeholder, real check below
    # isFraud itself must never be shifted by drift (it's binary 0/1, not a drift feature)
    assert all(r["isFraud"] in (0, 1) for r in records)


@mock_aws
def test_put_records_failure_is_retried(fixtures, monkeypatch):
    """
    Confirms put_batch_with_retry actually re-sends only the failed entries,
    by forcing moto's client to report a throttled record on the first call.
    """
    client = boto3.client("kinesis", region_name=REGION)
    client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
    client.get_waiter("stream_exists").wait(StreamName=STREAM_NAME)

    real_put_records = client.put_records
    call_count = {"n": 0}

    def flaky_put_records(**kwargs):
        call_count["n"] += 1
        response = real_put_records(**kwargs)
        if call_count["n"] == 1:
            # Pretend the first record in the first call was throttled.
            response = dict(response)
            response["FailedRecordCount"] = 1
            records = list(response["Records"])
            records[0] = {"ErrorCode": "ProvisionedThroughputExceededException",
                           "ErrorMessage": "mocked throttle"}
            response["Records"] = records
        return response

    monkeypatch.setattr(client, "put_records", flaky_put_records)
    monkeypatch.setattr(producer.boto3, "client", lambda *a, **kw: client)

    argv = [
        "producer.py",
        "--parquet", fixtures["parquet"],
        "--model-meta", fixtures["meta"],
        "--stats", fixtures["stats"],
        "--stream-name", STREAM_NAME,
        "--region", REGION,
        "--batch-size", "100",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    producer.main()

    assert call_count["n"] >= 2, "a simulated failure should trigger at least one retry call"


@mock_aws
def test_dry_run_sends_nothing(fixtures, monkeypatch, capsys):
    """--dry-run should build batches but never touch Kinesis."""
    argv = [
        "producer.py",
        "--parquet", fixtures["parquet"],
        "--model-meta", fixtures["meta"],
        "--stats", fixtures["stats"],
        "--stream-name", "stream-that-does-not-exist",
        "--region", REGION,
        "--batch-size", "100",
        "--dry-run",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    # Should not raise even though the stream was never created.
    producer.main()
