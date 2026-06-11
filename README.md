# retail-AzureProject
#  End-to-End Azure Data Engineering Pipeline

A production-grade retail data pipeline built on Azure, implementing the **Medallion Architecture** to ingest, transform, and visualize data from multiple sources through an interactive Power BI dashboard.

---

##  Architecture Overview

```
┌─────────────────────┐     ┌──────────────────────┐
│   Azure SQL DB      │     │      REST API         │
│  (transactions,     │     │  (customer JSON data) │
│   stores, products) │     │                       │
└────────┬────────────┘     └──────────┬────────────┘
         │                             │
         └─────────────┬───────────────┘
                       │  Azure Data Factory
                       │  (Parallel Copy Activities)
                       ▼
          ┌────────────────────────┐
          │    ADLS Gen2           │
          │  ┌──────────────────┐  │
          │  │   bronze/        │  │  ← Raw Parquet files
          │  │   silver/        │  │  ← Cleaned Delta Lake
          │  │   gold/          │  │  ← Aggregated Delta Lake
          │  └──────────────────┘  │
          └────────────┬───────────┘
                       │  Azure Databricks
                       │  (PySpark Transformations)
                       ▼
          ┌────────────────────────┐
          │     Power BI           │
          │  Executive Dashboard   │
          └────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion & Orchestration | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Processing & Transformation | Azure Databricks (PySpark + Delta Lake) |
| Visualization | Power BI Desktop |
| Source Databases | Azure SQL Database, REST API (HTTP) |

---

##  Repository Structure

```
├── adf/
│   ├── pipelines/             # ADF pipeline JSON definitions
│   └── linked_services/       # Linked service configs (SQL, HTTP, ADLS Gen2)
├── databricks/
│   ├── 01_adls_mounting.py    # ADLS storage mount setup
│   ├── 02_silver_layer.py     # Bronze → Silver transformations & joins
│   └── 03_gold_layer.py       # Silver → Gold aggregation metrics
├── database/
│   └── ddls_and_inserts.sql   # Azure SQL DDL schema & seed scripts
├── powerbi/
│   └── retail_dashboard.pbix  # Power BI report file
└── README.md
```

---

##  Setup & Execution Guide

### Phase 1 — Resource Provisioning & SQL Setup

1. Create a unified **Azure Resource Group**.
2. Provision an **Azure SQL Database** (Basic/Development tier).
3. Open the **Azure Query Editor** and run the schema initialization script:

```sql
-- Products
CREATE TABLE products (
    product_id   INT PRIMARY KEY,
    product_name VARCHAR(100),
    category     VARCHAR(50),
    price        FLOAT
);

-- Stores
CREATE TABLE stores (
    store_id   INT PRIMARY KEY,
    store_name VARCHAR(100),
    location   VARCHAR(100)
);

-- Transactions
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    customer_id    INT,
    product_id     INT FOREIGN KEY REFERENCES products(product_id),
    store_id       INT FOREIGN KEY REFERENCES stores(store_id),
    quantity       INT,
    transaction_date DATE
);
```

---

### Phase 2 — Storage Setup (ADLS Gen2)

1. Provision a **Standard Storage Account**.
   >  **Critical:** Enable **Hierarchical Namespace** under the Advanced tab to activate full ADLS Gen2 features.

2. Create a root container named `retail`.

3. Set up the following directory structure inside the container:

```
retail/
├── bronze/
│   ├── customer/
│   ├── product/
│   ├── store/
│   └── transaction/
├── silver/
└── gold/
```

---

### Phase 3 — Ingestion via Azure Data Factory

Create an ADF pipeline with **four parallel Copy Data activities**:

| Source | Linked Service Type | Sink Path | Format |
|---|---|---|---|
| `transactions` table | Azure SQL | `bronze/transaction/` | Parquet |
| `products` table | Azure SQL | `bronze/product/` | Parquet |
| `stores` table | Azure SQL | `bronze/store/` | Parquet |
| Customer REST API | HTTP (Anonymous) | `bronze/customer/` | Parquet |

Run a **Debug** execution to validate end-to-end ingestion into ADLS Gen2.

---

### Phase 4 — Databricks Notebook Development

#### 1. Mount ADLS Storage (`01_adls_mounting.py`)

```python
dbutils.fs.mount(
    source="wasbs://retail@<YOUR_STORAGE_ACCOUNT_NAME>.blob.core.windows.net",
    mount_point="/mnt/retail_project",
    extra_configs={
        "fs.azure.account.key.<YOUR_STORAGE_ACCOUNT_NAME>.blob.core.windows.net": "<YOUR_ACCESS_KEY>"
    }
)
```

#### 2. Silver Layer Transformations (`02_silver_layer.py`)

Cleans raw data, casts column types, runs multi-table joins, and derives a `total_amount` metric.

```python
from pyspark.sql.functions import col

# Read and cast transactions
df_transactions = spark.read.parquet("/mnt/retail_project/bronze/transaction") \
    .withColumn("transaction_id", col("transaction_id").cast("integer")) \
    .withColumn("customer_id",    col("customer_id").cast("integer"))

# (Apply equivalent type casting for df_products, df_stores, df_customers)

# Join all tables and derive total_amount
df_silver = df_transactions \
    .join(df_customers, "customer_id", "inner") \
    .join(df_products,  "product_id",  "inner") \
    .join(df_stores,    "store_id",    "inner") \
    .withColumn("total_amount", col("quantity") * col("price"))

# Persist as Delta Lake
df_silver.write.mode("overwrite").format("delta").save("/mnt/retail_project/silver")
```

#### 3. Gold Layer Aggregations (`03_gold_layer.py`)

Aggregates Silver data into business-level KPIs for reporting.

```python
from pyspark.sql.functions import sum, count, avg

df_gold = df_silver.groupBy(
    "transaction_date", "product_id", "product_name",
    "category", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    count("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

df_gold.write.mode("overwrite").format("delta").save("/mnt/retail_project/gold")
```

---

##  Power BI Dashboard

The Gold layer dataset feeds an executive Power BI dashboard with the following visuals:

| Visual | Metric |
|---|---|
| KPI Cards | Total Sales ($), Total Quantity Sold, Transaction Count |
| Line Chart | Sales trends over time (`total_sales_amount` vs `transaction_date`) |
| Pie / Donut Charts | Revenue breakdown by store and product category |
| Bar Charts | Top-performing products and category demand analysis |

---

##  Configuration Notes

Before running any notebooks, replace the placeholder values below with your actual Azure resource details:

| Placeholder | Replace With |
|---|---|
| `<YOUR_STORAGE_ACCOUNT_NAME>` | Your ADLS Gen2 storage account name |
| `<YOUR_ACCESS_KEY>` | Storage account access key (rotate after use) |

>  **Security:** Avoid hardcoding credentials directly in notebooks. Use **Azure Key Vault** backed secrets with `dbutils.secrets.get()` in production environments.

---

## 📋 Medallion Architecture Summary

```
Bronze  →  Raw, un-cleansed Parquet files as ingested from source systems
Silver  →  Typed, deduplicated, joined Delta Lake tables with derived fields
Gold    →  Aggregated, business-ready Delta Lake tables optimised for BI consumption
```
