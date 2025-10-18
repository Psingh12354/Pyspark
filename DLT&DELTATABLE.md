# 🧠 **Databricks Delta Table & Delta Live Table (DLT) – Complete Notes**

---

## 🚀 **1️⃣ What is Delta Lake / Delta Table?**

**Delta Lake** is an open-source storage layer from Databricks that brings **ACID transactions** and **schema evolution** to **data lakes**.

A **Delta Table** is a table stored in **Delta Lake format**, built on top of **Parquet files** with a **transaction log (_delta_log)** that ensures reliability, versioning, and data consistency.

---

## ⚙️ **2️⃣ Key Features of Delta Table**

| Feature                 | Description                                   |
| ----------------------- | --------------------------------------------- |
| ✅ **ACID Transactions** | Ensures data consistency across reads/writes. |
| 🧾 **Time Travel**      | Query historical versions of data.            |
| 🧮 **Schema Evolution** | Automatically handle column changes.          |
| 🔁 **Upserts (MERGE)**  | Update existing records or insert new ones.   |
| 🧹 **Vacuum**           | Clean up old data files.                      |
| ⚡ **Performance**       | Optimized with data skipping and Z-ordering.  |

---

## 📁 **3️⃣ Delta Table Structure**

When you save data as Delta:

```
/mnt/delta/employee/
 ├── part-00000-xxxx.snappy.parquet
 ├── part-00001-xxxx.snappy.parquet
 └── _delta_log/
      ├── 00000000000000000000.json
      ├── 00000000000000000001.json
```

* **Data files:** stored as Parquet
* **_delta_log:** stores transaction history (like Git commits for data)

---

## 🧱 **4️⃣ Creating Delta Table**

### 🧩 Using PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DeltaExample").getOrCreate()

data = [("A1", "John", 5000), ("A2", "Mary", 7000)]
columns = ["emp_id", "name", "salary"]

df = spark.createDataFrame(data, columns)

# Write as Delta Table
df.write.format("delta").mode("overwrite").save("/mnt/delta/employee")
```

📁 Creates a Delta Table in `/mnt/delta/employee`.

---

### 🧩 Read the Table

```python
df_read = spark.read.format("delta").load("/mnt/delta/employee")
df_read.show()
```

**Output:**

```
+------+----+------+
|emp_id|name|salary|
+------+----+------+
|A1    |John|  5000|
|A2    |Mary|  7000|
+------+----+------+
```

---

## 🔁 **5️⃣ Update, Delete, Merge (Upsert)**

### 🔹 Update

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/delta/employee")

deltaTable.update(
    condition="emp_id = 'A1'",
    set={"salary": "6000"}
)
```

### 🔹 Delete

```python
deltaTable.delete("salary < 6000")
```

### 🔹 Merge (Upsert)

```python
new_data = [("A1", "John", 6500), ("A3", "Alice", 5500)]
new_df = spark.createDataFrame(new_data, ["emp_id", "name", "salary"])

deltaTable.alias("old").merge(
    new_df.alias("new"),
    "old.emp_id = new.emp_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## 🕓 **6️⃣ Time Travel**

### 🔹 View Version History

```python
deltaTable.history().show(truncate=False)
```

### 🔹 Read Old Version

```python
df_old = spark.read.format("delta").option("versionAsOf", 1).load("/mnt/delta/employee")
df_old.show()
```

**Output:**

```
+------+----+------+
|emp_id|name|salary|
+------+----+------+
|A1    |John|  5000|
|A2    |Mary|  7000|
+------+----+------+
```

---

## 🧩 **7️⃣ Schema Evolution**

When your schema changes (e.g., new column), use `mergeSchema=True`.

```python
new_df = spark.createDataFrame([("A4", "Eve", 8000, "HR")], ["emp_id", "name", "salary", "dept"])

new_df.write.format("delta") \
     .option("mergeSchema", "true") \
     .mode("append") \
     .save("/mnt/delta/employee")
```

---

## 🧹 **8️⃣ Vacuum (Cleanup Old Versions)**

```python
deltaTable.vacuum(retentionHours=0)
```

⚠️ Removes files not referenced in the latest Delta snapshot.

---

# 🧩 **Now Let’s Move to Delta Live Tables (DLT)**

---

## 🚀 **9️⃣ What is Delta Live Table (DLT)?**

**Delta Live Tables (DLT)** is a **Databricks-managed ETL framework** built on top of Delta Lake.
It simplifies **data pipeline creation, dependency management, quality checks, and incremental updates**.

In simple words:

> **Delta Live Table = Automated ETL pipeline that writes Delta Tables**

---

## ⚙️ **10️⃣ DLT Key Features**

| Feature                         | Description                              |
| ------------------------------- | ---------------------------------------- |
| ✅ **Declarative Pipeline**      | Define *what to do*, not *how*.          |
| 🔄 **Incremental Updates**      | Auto-refresh when new data arrives.      |
| 🧩 **Data Quality Checks**      | Define `EXPECT` conditions.              |
| 🔗 **Dependency Tracking**      | Automatically builds execution DAG.      |
| 🧾 **Lineage Tracking**         | Visual graph showing table dependencies. |
| 🧮 **Automated Error Handling** | Retries failed operations.               |

---

## 🧠 **11️⃣ DLT Concepts**

| Term                | Meaning                                              |
| ------------------- | ---------------------------------------------------- |
| `@dlt.table`        | Defines a table that persists results as Delta Table |
| `@dlt.view`         | Defines a view (temporary transformation)            |
| `dlt.expect()`      | Defines data quality rule                            |
| `live.<table_name>` | Refers to previously defined DLT tables              |

---

## 🧱 **12️⃣ Example DLT Pipeline**

Below is a **Python DLT notebook** example 👇

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# 1️⃣ Bronze Table - Raw Data
@dlt.table(
  comment="Raw customer data"
)
def bronze_customers():
    return spark.read.json("/mnt/raw/customers")

# 2️⃣ Silver Table - Clean Data
@dlt.table(
  comment="Cleaned customer data"
)
def silver_customers():
    df = dlt.read("bronze_customers")
    return df.filter(col("email").contains("@")) \
             .withColumn("updated_at", current_timestamp())

# 3️⃣ Gold Table - Aggregated Data
@dlt.table(
  comment="Customer spend by country"
)
def gold_customer_spend():
    df = dlt.read("silver_customers")
    return df.groupBy("country").sum("spend")
```

---

## 📊 **13️⃣ Expected Output**

**Bronze Table (`live.bronze_customers`)**

```
+----+-----------+-------------+
|id  |name       |email        |
+----+-----------+-------------+
|1   |John Smith |john@mail.com|
|2   |Mary Adams |             |
+----+-----------+-------------+
```

**Silver Table (`live.silver_customers`)**

```
+----+-----------+-------------+-------------------+
|id  |name       |email        |updated_at         |
+----+-----------+-------------+-------------------+
|1   |John Smith |john@mail.com|2025-10-18 12:45:00|
+----+-----------+-------------+-------------------+
```

**Gold Table (`live.gold_customer_spend`)**

```
+---------+-----------+
|country  |sum(spend) |
+---------+-----------+
|USA      |  45000    |
|UK       |  32000    |
+---------+-----------+
```

---

## 🧮 **14️⃣ Data Quality (Optional)**

You can enforce data rules using **`dlt.expect()`**:

```python
@dlt.table
@dlt.expect("valid_email", "email LIKE '%@%'")
def silver_customers():
    df = dlt.read("bronze_customers")
    return df.filter(col("email").isNotNull())
```

* Invalid records are automatically captured in **DLT event logs**.

---

## 🧰 **15️⃣ Delta Table vs Delta Live Table – Quick Recap**

| Feature      | Delta Table                | Delta Live Table                   |
| ------------ | -------------------------- | ---------------------------------- |
| Type         | Storage Format             | Managed Pipeline                   |
| Built On     | Delta Lake                 | Delta Lake + Databricks            |
| Purpose      | Store data reliably        | Build & orchestrate data pipelines |
| Create With  | `df.write.format("delta")` | `@dlt.table` decorator             |
| Automation   | Manual                     | Fully managed                      |
| Data Quality | Manual logic               | Built-in `dlt.expect()`            |
| Lineage      | Manual tracking            | Auto lineage graph                 |
| Used For     | Batch, streaming, ETL      | Automated ETL pipelines            |

---

## 🧠 **16️⃣ Real-World Use Case Flow**

**Bronze → Silver → Gold Architecture (Lakehouse):**

```
Raw Data (Bronze)
    ↓
Cleaned / Validated (Silver)
    ↓
Aggregated / Business Ready (Gold)
```

| Layer      | Purpose             | Example             |
| ---------- | ------------------- | ------------------- |
| **Bronze** | Raw ingestion       | Kafka, Blob, JSON   |
| **Silver** | Clean and enrich    | Filtering, joins    |
| **Gold**   | Aggregate and serve | Reports, dashboards |

👉 DLT automates this pipeline using **Delta Tables** at each stage.

---

## 🧩 **17️⃣ Example Query in Databricks SQL**

```sql
SELECT * FROM live.gold_customer_spend ORDER BY sum(spend) DESC;
```

---

## 🎯 **18️⃣ Summary**

| Concept            | Meaning                                                          |
| ------------------ | ---------------------------------------------------------------- |
| **Delta Table**    | Data format that ensures ACID, schema evolution, and versioning. |
| **DLT**            | Pipeline system that automates ETL to build Delta Tables.        |
| **Best Practice**  | Use DLT for orchestration, Delta Tables for storage.             |
| **Common Pattern** | Bronze → Silver → Gold using DLT with Delta Tables underneath.   |
