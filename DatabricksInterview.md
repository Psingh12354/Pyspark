## ⚙️  Scenario-Based, Delta Lake, and ETL / Data Pipeline Interview Q&A**

---

### **1️⃣ Describe a challenging data engineering project you’ve worked on. What technologies did you use, and how did you overcome obstacles?**

**Answer (sample structure):**

> I worked on an ETL pipeline in Azure Databricks to process daily sales data from multiple regions.
>
> * **Tech stack:** Azure Data Lake (storage), Databricks (PySpark), Delta Lake (for ACID), and Synapse (reporting).
> * **Challenge:** The raw files had inconsistent schemas and data duplicates.
> * **Solution:**
>
>   * Used **Auto Loader** with schema evolution to handle schema drift.
>   * Implemented **Delta MERGE** for deduplication and incremental loads.
>   * Optimized Spark jobs using **partitioning** and **broadcast joins**.
> * Result: Reduced processing time from 45 mins to 10 mins.

✅ **Tip:** Always mention data volume, frequency, bottlenecks, and impact (e.g., improved speed or accuracy).

---

### **2️⃣ How do you handle data ingestion and transformation in a cloud environment?**

**Answer:**
Typical ETL flow in **Azure Databricks**:

1. **Ingestion:**

   * Use **Azure Data Factory** or **Databricks Auto Loader** to load from sources (ADLS, APIs, SQL).
2. **Transformation:**

   * Use **PySpark** for cleaning, joining, and aggregations.
   * Apply **Delta Lake** for incremental updates.
3. **Load:**

   * Write processed data into **Synapse**, **Power BI**, or **Delta tables** for analytics.

📘 Example:

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load("/mnt/raw/sales/")
df_clean = df.dropna().withColumn("profit", df.revenue - df.cost)
df_clean.writeStream.format("delta").option("checkpointLocation", "/mnt/checkpoints/sales").start("/mnt/processed/sales")
```

---

### **3️⃣ Explain the concept of Delta Lake and its benefits in data engineering workflows.**

**Answer:**
**Delta Lake** adds **ACID transactions**, **schema enforcement**, and **time travel** to your data lake.
It bridges the gap between **data lakes and data warehouses**.

**Key Features:**

* **ACID Transactions:** Reliable reads/writes even during concurrent operations.
* **Schema Evolution:** Automatically adapts to new columns.
* **Time Travel:** Access older versions of data (`VERSION AS OF`).
* **Merge Operations:** Support for `UPSERT` patterns.

📘 **Example:**

```python
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/delta/customers")

deltaTable.alias("t").merge(
    updates.alias("s"),
    "t.cust_id = s.cust_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

✅ **Result:** Perfect for incremental ETL or CDC (Change Data Capture) pipelines.

---

### **4️⃣ How do you implement incremental data loading in Databricks?**

**Answer:**
Incremental load = load only **new or changed data**.

**Steps:**

1. Maintain **watermark** column (e.g., `last_updated_date`).
2. Read only new data since the last load:

```python
new_data = spark.read.parquet("/mnt/raw/").filter("last_updated_date > '2025-10-01'")
```

3. Use **MERGE INTO** with Delta Lake:

```python
deltaTable.alias("target").merge(
    new_data.alias("src"),
    "target.id = src.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

✅ Efficient, avoids reprocessing entire dataset.

---

### **5️⃣ What is schema drift, and how do you handle it in Databricks?**

**Answer:**
**Schema drift** occurs when incoming data changes structure (e.g., new columns added).

✅ **Solutions:**

* Use **Auto Loader** with `cloudFiles.schemaEvolutionMode = addNewColumns`.
* Use `mergeSchema=True` while writing Delta tables:

```python
df.write.option("mergeSchema", "true").format("delta").mode("append").save("/mnt/delta/customers")
```

* Keep a **metadata version** table to track schema changes.

---

### **6️⃣ How would you design a data pipeline for daily ingestion of sales data into Delta Lake?**

**Answer:**
**ETL Architecture:**

1. **Raw Zone:**

   * Ingest CSVs via Auto Loader to `/mnt/raw/sales/`.
2. **Bronze Zone (Staging):**

   * Store raw ingested data (minimal transformations).
3. **Silver Zone (Cleansed):**

   * Apply deduplication, type casting, and joins.
4. **Gold Zone (Aggregated):**

   * Create summarized tables for analytics (e.g., daily revenue per region).

✅ **Tools:** Databricks Jobs + Delta Tables + Power BI for reporting.

📊 **Data Lakehouse Pattern:**
ADLS (storage) ➜ Databricks (ETL) ➜ Delta Lake (curated storage) ➜ Power BI (reporting)

---

### **7️⃣ How do you handle data deduplication in PySpark?**

**Answer:**
Use `dropDuplicates()`:

```python
df_clean = df.dropDuplicates(["customer_id", "order_date"])
```

Or using **Delta MERGE** for deduplication:

```python
deltaTable.alias("t").merge(
    df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

✅ Always combine with **window functions** or **timestamps** to keep the latest record.

---

### **8️⃣ How do you schedule and orchestrate Databricks jobs?**

**Answer:**

* Use **Databricks Jobs UI** to schedule notebooks or workflows.
* Orchestrate via **Azure Data Factory (ADF)** pipelines.
* Use **Databricks REST API** for automation in CI/CD pipelines.

✅ Common practice:
ADF pipeline triggers → Databricks job → writes to Delta → triggers downstream reporting refresh.

---

### **9️⃣ How do you monitor and handle failures in data pipelines?**

**Answer:**

* Use **Databricks Job Run History** and **Cluster Logs**.
* Log errors using `try-except` in PySpark:

```python
try:
    df = spark.read.csv("/mnt/raw/data.csv")
except Exception as e:
    print(f"Error: {e}")
```

* Integrate **Azure Monitor**, **Log Analytics**, or **Databricks Alerting**.
* Send **email or Teams notifications** via ADF or Databricks webhooks.

---

### **🔟 What is the Medallion Architecture in Databricks?**

**Answer:**
It’s a best-practice **data architecture pattern** for structured pipelines.

| Layer      | Purpose                    | Example                 |
| ---------- | -------------------------- | ----------------------- |
| **Bronze** | Raw data                   | Ingested CSVs           |
| **Silver** | Cleaned + validated data   | Filtered + deduplicated |
| **Gold**   | Aggregated + business data | Metrics, dashboards     |

✅ Simplifies debugging and enables scalable data pipelines.

---

### **1️⃣1️⃣ How do you handle slowly changing dimensions (SCD) in Databricks?**

**Answer:**
Use **Delta MERGE** with conditions.

📘 **Example (SCD Type 2):**

```python
from pyspark.sql.functions import current_date

deltaTable.alias("t").merge(
    updates.alias("s"),
    "t.cust_id = s.cust_id AND t.is_active = True"
).whenMatchedUpdate(set={"is_active": "False", "end_date": "current_date()"}) \
 .whenNotMatchedInsert(values={
     "cust_id": "s.cust_id",
     "name": "s.name",
     "is_active": "True",
     "start_date": "current_date()"
 }).execute()
```

✅ Maintains full change history for analytical tracking.

---

### **1️⃣2️⃣ How do you ensure data quality in your ETL pipelines?**

**Answer:**

* **Validation rules:** Check nulls, data types, duplicates.
* **Expectations:** Use **Delta Live Tables (DLT)** with `EXPECT` clauses.
* **Data profiling:** Capture summary statistics.
* **Alerts:** Trigger alerts for anomalies or threshold breaches.

📘 Example using DLT:

```sql
CREATE LIVE TABLE clean_data
AS SELECT * FROM raw_data
WHERE EXPECT(revenue > 0, 'Revenue must be positive');
```

---

### **1️⃣3️⃣ How do you manage and secure credentials in Databricks?**

**Answer:**

* Use **Azure Key Vault-backed Databricks secrets**:

  ```bash
  databricks secrets create-scope --scope myScope --scope-backend-type AZURE_KEYVAULT
  ```
* Access secrets in code:

  ```python
  token = dbutils.secrets.get(scope="myScope", key="apiKey")
  ```

✅ Prevents exposing sensitive info like passwords or keys in notebooks.

---

### **1️⃣4️⃣ Explain how you’d move data from on-prem SQL Server to Databricks.**

**Answer:**

1. **Extract:** Use **ADF Copy Activity** or **JDBC** to connect to on-prem SQL Server.
2. **Load:** Write data into **Azure Data Lake** (as Parquet/CSV).
3. **Transform:** Use **Databricks PySpark** for cleansing and aggregation.
4. **Store:** Save in **Delta Lake** for downstream analytics.

✅ **Bonus:** Incremental load with timestamp column (`modified_date`).

---

### **1️⃣5️⃣ How would you handle real-time streaming data in Databricks?**

**Answer:**
Use **Structured Streaming** with Delta Sink.

📘 **Example:**

```python
stream = spark.readStream.format("kafka") \
    .option("subscribe", "topic1") \
    .load()

transformed = stream.selectExpr("CAST(value AS STRING)")
transformed.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/stream") \
    .start("/mnt/delta/stream_output")
```

✅ Handles real-time data from Kafka or Event Hub efficiently.

---

