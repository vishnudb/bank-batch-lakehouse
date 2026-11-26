# Databricks notebook source
import random, uuid
import pandas as pd
from faker import Faker  # if Faker isn't installed, use random names instead

fake = Faker()
random.seed(42)
base = "/Volumes/batch_catalog/bronze/bank"  # change to your volume path

customers = pd.DataFrame({
    "customer_id": range(1, 501),
    "name": [fake.name() for _ in range(500)],
    "city": [fake.city() for _ in range(500)],
    "segment": [random.choice(["Retail", "Premium", "Corporate"]) for _ in range(500)],
})
accounts = pd.DataFrame({
    "account_id": range(1001, 1801),
    "customer_id": [random.randint(1, 500) for _ in range(800)],
    "branch": [random.choice(["Chennai", "Coimbatore", "Mumbai", "Delhi", "Pune"]) for _ in range(800)],
    "account_type": [random.choice(["SAVINGS", "CURRENT"]) for _ in range(800)],
})
rows = []
for i in range(5000):
    rows.append({
        "txn_id": f"T{i:06d}",
        "account_id": random.randint(1001, 1810),            # a few IDs don't exist
        "txn_date": fake.date_between("-30d", "today").strftime(random.choice(["%Y-%m-%d", "%d/%m/%Y"])),
        "amount": random.choice([None, "", -50, round(random.uniform(10, 90000), 2)]),
        "txn_type": random.choice(["credit", "DEBIT", "Debit", "CREDIT"]),
        "status": random.choice(["SUCCESS", "success", "FAILED", "PENDING"]),
    })
txns = pd.DataFrame(rows)
txns = pd.concat([txns, txns.sample(100)])                    # 100 duplicate rows

import os; os.makedirs(base, exist_ok=True)
customers.to_csv(f"{base}/customers.csv", index=False)
accounts.to_csv(f"{base}/accounts.csv", index=False)
txns.to_csv(f"{base}/transactions.csv", index=False)