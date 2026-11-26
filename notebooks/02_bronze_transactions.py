# Databricks notebook source
#incremental batch auto loader
from pyspark.sql import functions as F

chk = "/Volumes/batch_catalog/bronze/bank/checkpoints/bronze_transaction"
cat  = "batch_catalog"
base = "/Volumes/batch_catalog/bronze/bank/landing"

dbutils.widgets.text("run_date", "2026-11-24")
run_date = dbutils.widgets.get("run_date")

(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", False)
    .option("cloudFiles.schemaLocation", chk)
    .load(f"{base}/transactions/")
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("batch_date", F.lit(run_date))
 .writeStream
    .option("checkpointLocation", chk)
    .trigger(availableNow=True)
    .toTable(f"{cat}.bank_bronze.bronze_transactions"))
