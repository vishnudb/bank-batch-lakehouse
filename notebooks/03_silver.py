# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

cat = "batch_catalog"        # use the one catalog where ALL your bronze tables live
b = f"{cat}.bank_bronze"
s = f"{cat}.bank_silver"

(spark.table(f"{b}.bronze_customers")
    .select(F.col("customer_id").cast("int").alias("customer_id"),
            F.initcap(F.trim("name")).alias("name"),
            F.trim("city").alias("city"),
            F.upper(F.trim("segment")).alias("segment"))
    .dropDuplicates(["customer_id"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{s}.silver_customers"))

(spark.table(f"{b}.bronze_accounts")
    .select(F.col("account_id").cast("int").alias("account_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim("branch").alias("branch"),
            F.upper(F.trim("account_type")).alias("account_type"))
    .dropDuplicates(["account_id"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{s}.silver_accounts"))

raw = spark.table(f"{b}.bronze_transactions")

typed = (raw
    .withColumn("amount", F.expr("try_cast(nullif(trim(amount), '') as decimal(12,2))"))
    .withColumn("txn_date", F.to_date(F.coalesce(
        F.expr("try_to_timestamp(txn_date, 'yyyy-MM-dd')"),
        F.expr("try_to_timestamp(txn_date, 'dd/MM/yyyy')"))))
    .withColumn("account_id", F.col("account_id").cast("int"))
    .withColumn("txn_type", F.upper(F.trim("txn_type")))
    .withColumn("status", F.upper(F.trim("status"))))

# keep the latest copy of each transaction (handles the re-sent rows)
w = Window.partitionBy("txn_id").orderBy(F.col("ingest_ts").desc())
latest = (typed.withColumn("rn", F.row_number().over(w))
               .filter("rn = 1").drop("rn"))
latest.count()      # expect 170


accounts = spark.table(f"{s}.silver_accounts").select("account_id", "branch", "customer_id")
customers = spark.table(f"{s}.silver_customers").select("customer_id", "city", "segment")

checked = (latest.join(accounts.select("account_id").withColumn("acct_ok", F.lit(True)),
                       "account_id", "left")
    .withColumn("reject_reason",
        F.when(F.col("amount").isNull(), "amount missing or invalid")
         .when(F.col("amount") <= 0, "amount not positive")
         .when(F.col("txn_date").isNull(), "invalid date")
         .when(F.col("acct_ok").isNull(), "unknown account_id")))

rejected = checked.filter("reject_reason IS NOT NULL").drop("acct_ok")
valid = (checked.filter("reject_reason IS NULL").drop("acct_ok", "reject_reason")
    .join(accounts, "account_id", "left")
    .join(customers, "customer_id", "left")
    .select("txn_id", "account_id", "customer_id", "branch", "city", "segment",
            "txn_date", "amount", "txn_type", "status", "source_file", "ingest_ts"))



 # rejected is rebuilt from bronze each run, so overwrite keeps it idempotent
(rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{s}.silver_transactions_rejected"))

target = f"{s}.silver_transactions"
if not spark.catalog.tableExists(target):
    valid.write.format("delta").saveAsTable(target)
else:
    (DeltaTable.forName(spark, target).alias("t")
        .merge(valid.alias("s"), "t.txn_id = s.txn_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())           
