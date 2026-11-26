# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "2026-11-24")
run_date = dbutils.widgets.get("run_date")

cat  = "batch_catalog"
base = "/Volumes/batch_catalog/bronze/bank/landing"

def full_refresh(name):
    df = (spark.read.option("header", True).option("inferSchema", False)
          .csv(f"{base}/{name}/{name}.csv")
          .withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("batch_date", F.lit(run_date)))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{cat}.bank_bronze.bronze_{name}"))

for t in ["customers", "accounts"]:
    full_refresh(t)
