# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

cat = "batch_catalog"
s = f"{cat}.bank_silver"
g = f"{cat}.bank_gold"
txn = spark.table(f"{s}.silver_transactions")

def save(df, name):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(f"{g}.{name}"))
    
branch_daily = (txn.filter("status = 'SUCCESS'")
    .groupBy("branch", "txn_date")
    .agg(F.count("*").alias("txn_count"),
         F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("total_credits"),
         F.sum(F.when(F.col("txn_type") == "DEBIT",  F.col("amount")).otherwise(0)).alias("total_debits"))
    .withColumn("net_flow", F.col("total_credits") - F.col("total_debits")))
save(branch_daily, "gold_branch_daily_summary")


monthly = (txn.filter("status = 'SUCCESS' AND txn_type = 'DEBIT'")
    .withColumn("month", F.date_trunc("month", "txn_date").cast("date"))
    .groupBy("customer_id", "segment", "month")
    .agg(F.sum("amount").alias("total_spend"),
         F.count("*").alias("debit_count")))

w = Window.partitionBy("segment", "month").orderBy(F.col("total_spend").desc())
save(monthly.withColumn("rank_in_segment", F.dense_rank().over(w)),
     "gold_customer_monthly_spend")


high_value = (txn.filter("status = 'SUCCESS' AND txn_type = 'DEBIT' AND amount > 50000")
    .select("txn_id", "customer_id", "account_id", "branch", "txn_date", "amount"))
save(high_value, "gold_high_value_flags")


failed_rate = (txn.groupBy("branch")
    .agg(F.count("*").alias("total_txns"),
         F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_txns"))
    .withColumn("failed_pct", F.round(F.col("failed_txns") * 100 / F.col("total_txns"), 2)))
save(failed_rate, "gold_failed_txn_rate")

