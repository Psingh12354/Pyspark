# 🧱 End-to-End Data Quality Pipeline with Quarantine Table in PySpark (Databricks + Delta Lake)

---

## 📘 Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Lake Zones](#data-lake-zones)
4. [Data Quality Rules](#data-quality-rules)
5. [Splitting Data into Valid and Quarantine Tables](#splitting-data-into-valid-and-quarantine-tables)
6. [Using `exceptAll` for Quarantine](#using-exceptall-for-quarantine)
7. [Using Deequ for Validation](#using-deequ-for-validation)
8. [Tracking Data Quality Metrics](#tracking-data-quality-metrics)
9. [Reprocessing Quarantined Data](#reprocessing-quarantined-data)
10. [Folder Structure](#folder-structure)
11. [Summary](#summary)
12. [Enhancements](#enhancements)

---

## 🧠 Overview

**Data Quality** ensures that your data is:

* ✅ **Accurate** (values are correct)
* ✅ **Complete** (no missing fields)
* ✅ **Consistent** (uniform across systems)
* ✅ **Valid** (matches business rules)
* ✅ **Unique** (no duplicates)

A strong data quality pipeline separates **good data** from **bad data**, ensures traceability, and helps reprocess quarantined data later.

---

## 🏗️ Architecture

```
                ┌───────────────────────────┐
                │       Source Systems       │
                └────────────┬──────────────┘
                             │
                             ▼
                  ┌────────────────────┐
                  │  🟤 Bronze Layer   │ → Raw Ingested Data
                  └────────┬───────────┘
                           │
                           ▼
                 ┌──────────────────────┐
                 │  ⚪ Silver Layer      │ → Valid & Clean Data
                 └────────┬─────────────┘
                           │
            ┌──────────────┴──────────────┐
            ▼                             ▼
   ┌──────────────────┐         ┌────────────────────┐
   │ 🟥 Quarantine     │         │ 🟡 Gold Layer       │
   │ Invalid Data      │         │ Aggregated Reports  │
   └──────────────────┘         └────────────────────┘
```

---

## 📂 Data Lake Zones

| Zone              | Description              | Example Table             |
| ----------------- | ------------------------ | ------------------------- |
| 🟤 **Bronze**     | Raw, unvalidated data    | `bronze.transactions`     |
| ⚪ **Silver**      | Cleaned, validated data  | `silver.transactions`     |
| 🟥 **Quarantine** | Invalid/rejected records | `quarantine.transactions` |
| 🟡 **Gold**       | Aggregated business data | `gold.sales_summary`      |

---

## 🧩 Data Quality Rules

| Rule | Column             | Description                                        |
| ---- | ------------------ | -------------------------------------------------- |
| R1   | `amount`           | Must not be null or negative                       |
| R2   | `transaction_date` | Must be a valid date                               |
| R3   | `email`            | Must match email format                            |
| R4   | `duplicates`       | Remove duplicate `(customer_id, transaction_date)` |

---

## 🧪 Splitting Data into Valid and Quarantine Tables

To move incorrect data to a **quarantine table** in PySpark,
you simply **filter the original DataFrame** into two separate DataFrames:

* ✅ **Valid records** → Meet all data quality rules
* 🟥 **Invalid records (quarantine)** → Fail one or more rules

Then write each DataFrame to its **respective Delta table**.

---

### 🥉 Step 1: Load Raw Data (Bronze)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataQualityWithQuarantine").getOrCreate()

data = [
    (101, 250, "2025-10-30", "a@gmail.com"),
    (102, None, "2025-10-30", "b@gmail.com"),
    (103, 2000, "2025-13-30", "c@@gmail.com"),
    (101, 250, "2025-10-30", "a@gmail.com")
]
columns = ["customer_id", "amount", "transaction_date", "email"]

df_bronze = spark.createDataFrame(data, columns)
df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.transactions")
```

---

### ⚙️ Step 2: Apply Data Quality Rules

```python
from pyspark.sql.functions import col, to_date

# ✅ Valid Data (Silver)
df_valid = (
    df_bronze
    .dropDuplicates(["customer_id", "transaction_date"])
    .filter(col("amount").isNotNull() & (col("amount") > 0))
    .filter(to_date(col("transaction_date"), "yyyy-MM-dd").isNotNull())
    .filter(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
)

# 🟥 Invalid Data (Quarantine)
df_invalid = df_bronze.filter(~(
    col("amount").isNotNull() & 
    (col("amount") > 0) & 
    to_date(col("transaction_date"), "yyyy-MM-dd").isNotNull() & 
    col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
))
```

---

### 💾 Step 3: Write to Respective Tables

```python
# ✅ Valid data → Silver Layer
df_valid.write.format("delta").mode("overwrite").saveAsTable("silver.transactions")

# 🟥 Invalid data → Quarantine Table
df_invalid.write.format("delta").mode("overwrite").saveAsTable("quarantine.transactions")
```

This is the **core pattern** for moving incorrect data to a **quarantine table**.

---

## 🔄 Using `exceptAll` for Quarantine (Alternative Approach)

You can also get invalid records using PySpark’s `exceptAll()` for better auditing:

```python
df_invalid = df_bronze.exceptAll(df_valid)
```

This automatically captures all records that are in `df_bronze` but **not** in `df_valid` — preserving duplicates and schema.

---

## 🧠 Using Deequ for Data Validation

You can integrate **Amazon Deequ** (data quality framework) for automated validation:

1. Install Deequ on Databricks:

   ```
   com.amazon.deequ:deequ:2.0.3-spark-3.3
   ```

2. Example Deequ validation:

   ```python
   from py4j.java_gateway import java_import
   java_import(spark._jvm, "com.amazon.deequ.checks.*")
   java_import(spark._jvm, "com.amazon.deequ.verification.*")

   verification_suite = (
       spark._jvm.com.amazon.deequ.VerificationSuite()
           .onData(df_bronze._jdf)
           .addCheck(
               spark._jvm.com.amazon.deequ.checks.Check(
                   spark._jvm.com.amazon.deequ.checks.CheckLevel.Error, "Data Quality Checks"
               )
               .isComplete("amount")
               .isNonNegative("amount")
               .isComplete("email")
               .hasPattern("email", "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}")
           )
           .run()
   )

   results = verification_suite.checkResultsAsDataFrame(spark._jsparkSession)
   results.show(truncate=False)
   ```

Deequ automatically generates **validation reports** and **metrics** for each rule.

---

## 📊 Tracking Data Quality Metrics

```python
total_records = df_bronze.count()
valid_records = df_valid.count()
invalid_records = df_invalid.count()

dq_score = (valid_records / total_records) * 100

print(f"✅ Data Quality Score: {dq_score:.2f}%")
print(f"Valid: {valid_records}, Invalid: {invalid_records}")
```

🧮 **Example Output:**

```
✅ Data Quality Score: 50.00%
Valid: 2, Invalid: 2
```

---

## 🧰 Reprocessing Quarantined Data

Sometimes, quarantined data can be fixed and reprocessed later.

Example: Fill missing amounts or correct invalid emails.

```python
from pyspark.sql.functions import when, regexp_replace, lit

df_quarantine = spark.table("quarantine.transactions")

df_repaired = (
    df_quarantine
    .withColumn("amount", when(col("amount").isNull(), lit(100)).otherwise(col("amount")))
    .withColumn("email", regexp_replace(col("email"), "@@", "@"))
)

# Move repaired data to Silver
df_repaired.write.format("delta").mode("append").saveAsTable("silver.transactions")
```

---

## 📁 Folder Structure (Typical in Databricks)

```
/mnt/datalake/
 ├── bronze/
 │    └── transactions/
 ├── silver/
 │    └── transactions/
 ├── quarantine/
 │    └── transactions/
 └── gold/
      └── sales_summary/
```

---

## 📘 Summary

| Step | Action                          | Output Table              | Tool            |
| ---- | ------------------------------- | ------------------------- | --------------- |
| 1    | Load raw data                   | `bronze.transactions`     | PySpark         |
| 2    | Apply validation rules          | Valid / Invalid split     | PySpark filters |
| 3    | Move invalid data               | `quarantine.transactions` | PySpark / Delta |
| 4    | Store valid data                | `silver.transactions`     | Delta           |
| 5    | Track quality metrics           | % Valid vs Invalid        | Python          |
| 6    | (Optional) Automated validation | Deequ                     | Spark Library   |
| 7    | Reprocess invalids              | Move fixed data to Silver | PySpark         |

---

## 🚀 Enhancements

| Enhancement                       | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| 🧱 **Delta Live Tables (DLT)**    | Use `@dlt.expect_or_drop()` for built-in data quality checks |
| 📊 **Databricks SQL Dashboard**   | Visualize DQ metrics (valid %, invalid %)                    |
| 🧠 **Deequ / Great Expectations** | For declarative quality validation                           |
| 🔔 **Alerting**                   | Notify if invalid % exceeds a threshold                      |
| 🔁 **Data Re-ingestion Pipeline** | Automate reprocessing of quarantined data                    |

---

## ✅ Final Takeaway

> **To move incorrect data to a quarantine table in PySpark:**
>
> 1. Define your data quality rules.
> 2. Split your DataFrame into **valid** and **invalid** sets using filters.
> 3. Write **valid records** to the Silver layer.
> 4. Write **invalid records** to the Quarantine table.
> 5. Optionally, validate and track metrics using **Deequ** or **Delta expectations**.

This is the **standard production pattern** used by Data Engineers in Databricks to maintain clean, reliable, and auditable data pipelines. 🚀

---

