# Databricks notebook source
cat = "batch_catalog"
bronze_distinct = spark.sql(f"SELECT COUNT(DISTINCT txn_id) c FROM {cat}.bank_bronze.bronze_transactions").first().c
valid     = spark.table(f"{cat}.bank_silver.silver_transactions").count()
rejected  = spark.table(f"{cat}.bank_silver.silver_transactions_rejected").count()
dupes     = spark.sql(f"SELECT COUNT(*) - COUNT(DISTINCT txn_id) d FROM {cat}.bank_silver.silver_transactions").first().d
gold_total = spark.sql(f"SELECT SUM(total_txns) t FROM {cat}.bank_gold.gold_failed_txn_rate").first().t

assert valid + rejected == bronze_distinct, f"valid+rejected ({valid+rejected}) != bronze distinct ({bronze_distinct})"
assert dupes == 0, f"{dupes} duplicate txn_ids in silver"
assert gold_total == valid, f"gold total {gold_total} != silver valid {valid}"
print("All checks passed")