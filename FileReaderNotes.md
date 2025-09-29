# 📘 PySpark & Databricks – File I/O, Tables, JSON Handling, Nested Data, Parse Modes, Permissions

---

## 🔹 1. Ways to Read Data in PySpark

PySpark allows reading data **directly from paths** or from **registered tables/views**.

### (a) Path-Based Reads

```python
df_csv     = spark.read.csv("/path/file.csv", header=True, inferSchema=True)
df_json    = spark.read.json("/path/file.json")
df_parquet = spark.read.parquet("/path/file.parquet")
df_orc     = spark.read.orc("/path/file.orc")
df_text    = spark.read.text("/path/file.txt")
```

👉 Use when data is stored as **raw files** in storage (S3, ADLS, DBFS, GCS).

---

### (b) Table-Based Reads

```python
df1 = spark.read.table("my_db.my_table")   # via Spark read API
df2 = spark.table("my_db.my_table")        # shorthand
df3 = spark.sql("SELECT * FROM my_db.my_table")  # SQL
```

👉 Use when table is **registered in Hive/Unity Catalog metastore**.

---

### (c) Temporary Views

```python
df.createOrReplaceTempView("temp_view")
df_temp = spark.table("temp_view")  # session-scoped

df.createGlobalTempView("global_view")
df_glob = spark.table("global_temp.global_view")  # cluster-wide
```

👉 Useful when you want to run **SQL on DataFrames** without saving to disk.

---

## 🔹 2. File Formats Supported

| Format            | Read Example                                       | Write Example                                    | Features                            | Best Use Case         |
| ----------------- | -------------------------------------------------- | ------------------------------------------------ | ----------------------------------- | --------------------- |
| **CSV**           | `spark.read.csv("file.csv", header=True)`          | `df.write.csv("out.csv", header=True)`           | Simple, human-readable, flexible    | Small data, exports   |
| **JSON**          | `spark.read.json("file.json")`                     | `df.write.json("out.json")`                      | Handles nested/array data           | API logs, events      |
| **Parquet**       | `spark.read.parquet("file.parquet")`               | `df.write.parquet("out.parquet")`                | Columnar, compressed, fast          | Analytics queries     |
| **ORC**           | `spark.read.orc("file.orc")`                       | `df.write.orc("out.orc")`                        | Hive-optimized                      | Hadoop/Hive workloads |
| **Avro**          | `spark.read.format("avro").load("file.avro")`      | `df.write.format("avro").save("out.avro")`       | Schema evolution support            | Streaming, Kafka      |
| **Text**          | `spark.read.text("file.txt")`                      | `df.write.text("out.txt")`                       | Line-based                          | Logs, plain text      |
| **Binary**        | `spark.read.format("binaryFile").load("/images")`  | ❌                                                | Reads binary blobs                  | Image/ML pipelines    |
| **Delta Lake** 🟢 | `spark.read.format("delta").load("/delta/events")` | `df.write.format("delta").save("/delta/events")` | ACID, schema evolution, time travel | Databricks Lakehouse  |

👉 **Best practice:** Use **Parquet or Delta** for large-scale analytics because they’re columnar and compressed.

---

## 🔹 3. Handling JSON & Nested Data

JSON often contains **nested structures (structs, arrays, maps)**. Spark provides tools to flatten or explode them.

### (a) Load Nested JSON

```python
df = spark.read.option("multiLine", True).json("/path/data.json")
```

👉 `multiLine=True` helps when JSON spans multiple lines.

---

### (b) Access Nested Fields

```python
df.select("id", "address.city", "address.zip").show()
```

👉 Use **dot notation** to drill into nested structs.

---

### (c) Explode Arrays

```python
from pyspark.sql.functions import explode
df.select("id", explode("phones").alias("phone")).show()
```

👉 **Explode** converts array elements into separate rows.

---

### (d) Define Explicit Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("zip", IntegerType(), True)
    ])),
    StructField("phones", ArrayType(StringType()), True)
])

df = spark.read.schema(schema).json("/path/data.json")
```

👉 Explicit schema is **faster** and avoids Spark’s expensive `inferSchema`.

---

### (e) Working with Structs, Arrays, Maps

* **Struct (nested object)**

```python
from pyspark.sql.functions import col
df.select(col("address.city").alias("city")).show()
```

* **Array (list of values)**

```python
from pyspark.sql.functions import size
df.select("id", size("phones").alias("phone_count")).show()
```

* **Map (key-value pairs)**

```python
# Example: {"properties": {"height":"5.6", "weight":"60"}}
df.select("properties.height", "properties.weight").show()
```

👉 Think of **Struct = object**, **Array = list**, **Map = dictionary**.

---

### (f) Flattening Nested JSON

```python
from pyspark.sql.functions import explode, col

df_flat = df.select(
    "id",
    "name",
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
    explode("phones").alias("phone")
)
```

👉 Convert nested structures into **flat tabular form**.

---

## 🔹 4. Parse Modes & Options

When reading **text-based formats (CSV, JSON)**, Spark provides parse modes.

### JSON & CSV Parse Modes

| Mode                     | Behavior                                          |
| ------------------------ | ------------------------------------------------- |
| **PERMISSIVE** (default) | Keeps corrupt records in `_corrupt_record` column |
| **DROPMALFORMED**        | Skips malformed rows entirely                     |
| **FAILFAST**             | Aborts job if any malformed row is found          |

```python
df = spark.read.option("mode", "DROPMALFORMED").json("/path/file.json")
```

👉 Use **PERMISSIVE** in production (safe), **FAILFAST** in testing (strict).

---

### CSV Extra Options

* `header=True` → first row as header
* `sep="|"` → delimiter
* `inferSchema=True` → auto column types
* `quote='"'`, `escape='\\'` → handle quoted values
* `multiLine=True` → parse multi-line CSVs

```python
df = spark.read.option("header", True) \
               .option("sep", "|") \
               .csv("/path/data.csv")
```

---

### JSON Extra Options

* `multiLine=True` → multi-line JSON
* `allowSingleQuotes=True` → accept `'key':'value'`
* `allowUnquotedFieldNames=True` → JSON keys without quotes
* `primitivesAsString=True` → numbers/booleans as strings
* `dropFieldIfAllNull=True` → drop all-null fields

```python
df = spark.read.option("multiLine", True) \
               .option("mode", "FAILFAST") \
               .json("/path/data.json")
```

---

### Parquet & ORC

* **Schema-driven** → fail if mismatch (no `_corrupt_record`).
* Options: `mergeSchema=True`, filter pushdown.

### Delta Lake

* **ACID transactions**
* Schema enforcement (`overwriteSchema=True`)
* Schema evolution (`mergeSchema=True`)
* No corrupt record handling.

---

## 🔹 5. Delta Lake Features

Delta = Parquet + ACID + Time Travel.

```python
# Write as Delta
df.write.format("delta").mode("overwrite").save("/mnt/delta/events")

# Time Travel
df_old = spark.read.format("delta").option("versionAsOf", 3).load("/mnt/delta/events")

# Merge (Upsert)
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/delta/events")
deltaTable.alias("t").merge(
    df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## 🔹 6. Write Modes

```python
df.write.mode("overwrite").parquet("/output")
df.write.mode("append").json("/output")
```

* **overwrite** → Replace old data
* **append** → Add new data
* **ignore** → Do nothing if exists
* **errorifexists** → Throw error if exists

---

## 🔹 7. Permissions & Security in Databricks

* **Table-level** (SQL)

```sql
GRANT SELECT ON TABLE my_db.my_table TO `user1`;
GRANT MODIFY ON TABLE my_db.my_table TO `analyst_group`;
```

* **Storage-level**

  * AWS → IAM roles, S3 bucket policies
  * Azure → RBAC, ACLs, SAS tokens
  * GCP → IAM roles

* **Databricks-specific**

  * Unity Catalog → central governance
  * Supports **row/column-level security**
  * Use `dbutils.secrets.get()` for keys & tokens

---

## 🔹 8. Format Comparison (Quick Interview Guide)

| Feature           | CSV         | JSON      | Parquet   | ORC          | Avro      | Delta Lake |
| ----------------- | ----------- | --------- | --------- | ------------ | --------- | ---------- |
| Human-readable    | ✅           | ✅         | ❌         | ❌            | ❌         | ❌          |
| Compression       | ❌           | ❌         | ✅         | ✅            | ✅         | ✅          |
| Schema Evolution  | ❌           | Limited   | ✅         | ✅            | ✅         | ✅          |
| Nested Data       | ❌           | ✅         | ✅         | ✅            | ✅         | ✅          |
| ACID Transactions | ❌           | ❌         | ❌         | ❌            | ❌         | ✅          |
| Time Travel       | ❌           | ❌         | ❌         | ❌            | ❌         | ✅          |
| Parse Modes       | ✅           | ✅         | ❌         | ❌            | ❌         | ❌          |
| Best Use Case     | Small files | Logs/APIs | Analytics | Hive queries | Streaming | Lakehouse  |

---

✅ **Quick Takeaways for Interview:**

* Use **PERMISSIVE/DROPMALFORMED/FAILFAST** only for text-based formats (CSV, JSON).
* **Parquet, ORC, Delta** → strongly typed, fail on mismatch.
* Flatten nested JSON using **dot notation, explode, and schema definition**.
* Prefer **Delta Lake** in Databricks for ACID + governance.

---

