"""
feature_engineering.py

AWS Glue (Spark) job: reads validated records from S3 PROCESSED, applies
feature engineering, writes to S3 FEATURES as Parquet. Replaces the EMR
Serverless PySpark step in the original design — same role in the pipeline,
run as an on-demand Glue job instead (no persistent cluster/application to
manage or forget to shut down).

This is a placeholder: feature logic is marked with a TODO. Deploy with
infra/deploy.sh (uploads this script to SCRIPTS_BUCKET and creates the Glue
job), then run on-demand with:

    aws glue start-job-run --job-name <GLUE_JOB_NAME> --region eu-north-1
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "PROCESSED_BUCKET", "FEATURES_BUCKET"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

processed_path = f"s3://{args['PROCESSED_BUCKET']}/"
features_path = f"s3://{args['FEATURES_BUCKET']}/"

df = spark.read.json(processed_path)  # processed records are written as .jsonl by the Lambda

# TODO: replace with real feature engineering (ratios, rolling aggregates,
# joins against reference data, etc). Placeholder below just demonstrates
# the read -> transform -> write shape with one example derived column.
if "amount" in df.columns and "oldbalanceOrg" in df.columns:
    df = df.withColumn(
        "amount_to_balance_ratio",
        F.when(F.col("oldbalanceOrg") > 0, F.col("amount") / F.col("oldbalanceOrg")).otherwise(0.0),
    )

df.write.mode("append").parquet(features_path)

job.commit()
