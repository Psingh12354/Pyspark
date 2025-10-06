Perfect ✅ — here’s a **complete and easy-to-understand Delta Lake notes guide** tailored for **Databricks / PySpark interviews** and real-world projects.
It’s written in a **GitHub README-style format**, with **examples, code, and explanations** — just like your previous DP-900 notes.

---

# 🧠 Delta Lake — Complete Notes (with Examples)

---

## 📘 **1. What is Delta Lake?**

**Delta Lake** is an open-source **storage layer** built on top of your existing **data lake** (like Azure Data Lake, S3, or GCS).
It brings **reliability, performance, and ACID transactions** to data lakes — turning them into **“Lakehouses.”**

### 🏗️ Key Features:

* ✅ **ACID Transactions** (Atomicity, Consistency, Isolation, Durability)
* 📜 **Schema Enforcement & Evolution**
* 🕓 **Time Travel**
* 🔄 **Upserts (MERGE), Deletes, Updates**
* 🧩 **Data Versioning**
* ⚡ **Optimized performance with Z-Ordering**
* ♻️ **Integrates seamlessly with Spark, Databricks, and cloud storage**

---

## 📁 **2. Delta Table Storage Structure**

When you create a Delta table, it looks like this inside your storage:

```
/mnt/delta/sales/
├── part-00000-xxxx.snappy.parquet
├── part-00001-yyyy.snappy.parquet
└── _delta_log/
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    ├── ...
```

* The **data files** are stored in **Parquet format**.
* The **`_delta_log`** directory contains **JSON log files** (transaction logs).
* Each JSON file records every **transaction** (insert/update/delete/merge).

---

## ⚙️ **3. Creating Sample Data**

Let’s start with some dummy data 👇

```python
data = [
    (1, "Alice", 1000),
    (2, "Bob", 1500),
    (3, "Charlie", 2000)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)
df.show()
```

📊 **Output:**

| id | name    | salary |
| -- | ------- | ------ |
| 1  | Alice   | 1000   |
| 2  | Bob     | 1500   |
| 3  | Charlie | 2000   |

---

## ✍️ **4. Writing Data to Delta Table**

```python
df.write.format("delta").mode("overwrite").save("/mnt/delta/employees")
```

✅ This command:

* Writes the DataFrame to a **Delta table** format.
* Creates a folder with `_delta_log`.

---

## 📖 **5. Reading Data from Delta Table**

```python
delta_df = spark.read.format("delta").load("/mnt/delta/employees")
delta_df.show()
```

💡 You can also create a SQL table:

```sql
CREATE TABLE employees USING DELTA LOCATION '/mnt/delta/employees';
SELECT * FROM employees;
```

---

## 🔁 **6. Write Modes in Delta Lake**

| Mode            | Description                 |
| --------------- | --------------------------- |
| `append`        | Adds new records            |
| `overwrite`     | Replaces existing data      |
| `ignore`        | Skips write if table exists |
| `errorifexists` | Fails if table exists       |
| `merge`         | Conditional insert/update   |

Example:

```python
df.write.format("delta").mode("append").save("/mnt/delta/employees")
```

---

## 🧱 **7. Delta Lake vs Parquet**

| Feature                   | Parquet    | Delta Lake         |
| ------------------------- | ---------- | ------------------ |
| ACID Transactions         | ❌ No       | ✅ Yes              |
| Time Travel               | ❌ No       | ✅ Yes              |
| Schema Enforcement        | ❌ No       | ✅ Yes              |
| Upserts & Deletes         | ❌ No       | ✅ Yes              |
| Data Versioning           | ❌ No       | ✅ Yes              |
| Performance Optimizations | ⚠️ Limited | ✅ ZORDER, OPTIMIZE |

---

## 🔍 **8. Time Travel (Versioning)**

Delta keeps **multiple versions** of data using transaction logs.

### ⏳ Query older data

```python
# Using version number
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/mnt/delta/employees")

# Using timestamp
df_time = spark.read.format("delta").option("timestampAsOf", "2025-10-05T10:00:00Z").load("/mnt/delta/employees")
```

### SQL version

```sql
SELECT * FROM employees VERSION AS OF 0;
```

---

## 🧩 **9. Schema Enforcement & Evolution**

### Schema Enforcement (Strict Checking)

If you try to write a column that doesn’t exist — Delta will throw an error.

### Schema Evolution (Allow Changes)

You can **add new columns automatically** by enabling mergeSchema:

```python
new_data = [(4, "David", 2500, "IT")]
new_df = spark.createDataFrame(new_data, ["id", "name", "salary", "dept"])

new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/employees")
```

---

## 🔄 **10. MERGE Operation (UPSERT)**

Used for **updating or inserting** data in a single transaction.

```sql
MERGE INTO employees AS t
USING updates AS s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET t.salary = s.salary
WHEN NOT MATCHED THEN
  INSERT (id, name, salary) VALUES (s.id, s.name, s.salary);
```

Equivalent PySpark code:

```python
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "/mnt/delta/employees")
updatesDF = spark.createDataFrame([(2, "Bob", 1800)], ["id", "name", "salary"])

deltaTable.alias("t").merge(
    updatesDF.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## 🧹 **11. VACUUM Command**

Cleans up old files no longer needed for time travel.

```sql
VACUUM employees RETAIN 168 HOURS;
```

🕐 Default retention: **7 days (168 hours)**
To force immediate cleanup (⚠️ risky):

```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM employees RETAIN 0 HOURS;
```

---

## ⚡ **12. OPTIMIZE & ZORDER**

Used to **compact small files** and **improve query speed**.

```sql
OPTIMIZE employees;
OPTIMIZE employees ZORDER BY (dept);
```

ZORDER sorts data by specific columns for faster queries.

---

## 🧰 **13. Change Data Feed (CDF)**

Tracks changes (inserts/updates/deletes) between table versions.

Enable it:

```sql
ALTER TABLE employees SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

Read changes:

```python
spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load("/mnt/delta/employees")
```

---

## 🔁 **14. Streaming with Delta**

### Write stream to Delta

```python
df.writeStream.format("delta") \
    .option("checkpointLocation", "/mnt/delta/checkpoints") \
    .start("/mnt/delta/stream_data")
```

### Read stream from Delta

```python
stream_df = spark.readStream.format("delta").load("/mnt/delta/stream_data")
```

---

## 💾 **15. Managed vs External Delta Tables**

| Type     | Storage Location                   | Example                                                              |
| -------- | ---------------------------------- | -------------------------------------------------------------------- |
| Managed  | Stored inside Databricks metastore | `CREATE TABLE employees (id INT) USING DELTA`                        |
| External | Stored in user-defined path        | `CREATE TABLE employees USING DELTA LOCATION '/mnt/delta/employees'` |

---

## 🧱 **16. Converting Parquet to Delta**

If you have an existing Parquet table:

```sql
CONVERT TO DELTA parquet.`/mnt/delta/parquet_data`
```

---

## 🧠 **17. Checkpoints & Transaction Logs**

* **Checkpoints** are Parquet summaries of transaction logs.
* They help Spark quickly rebuild table state during reads.
* Stored every **10 commits** by default.

---

## 🧩 **18. Handling Concurrent Writes**

Delta uses **transaction logs** with optimistic concurrency control.
If two jobs write simultaneously, one fails to avoid data corruption.

---

## 🧮 **19. Restoring a Table**

```sql
RESTORE TABLE employees TO VERSION AS OF 3;
```

---

## 🌐 **20. Delta Live Tables (DLT)**

DLT is a **Databricks-managed pipeline framework** built on Delta Lake.
It automates:

* Data ingestion,
* Quality enforcement (expectations),
* Pipeline orchestration.

---

## ✅ **21. Common Interview Scenarios**

| Scenario               | How to Handle                     |
| ---------------------- | --------------------------------- |
| Duplicate small files  | Use `OPTIMIZE`                    |
| Deleted wrong data     | Use `TIME TRAVEL` or `RESTORE`    |
| Schema mismatch        | Enable `mergeSchema`              |
| Need incremental loads | Use `MERGE` or `Change Data Feed` |
| Query slow             | Use `ZORDER` and caching          |

---

## 🧩 **22. Summary Table**

| Feature            | Command                             |
| ------------------ | ----------------------------------- |
| Create Delta Table | `df.write.format("delta").save()`   |
| Read Delta Table   | `spark.read.format("delta").load()` |
| Time Travel        | `option("versionAsOf", x)`          |
| Merge (Upsert)     | `MERGE INTO`                        |
| Vacuum             | `VACUUM table`                      |
| Optimize           | `OPTIMIZE table`                    |
| Z-Order            | `OPTIMIZE table ZORDER BY (col)`    |
| Change Data Feed   | `readChangeFeed = true`             |
| Restore            | `RESTORE TABLE ...`                 |

---

