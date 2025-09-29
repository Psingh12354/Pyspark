
# 📘 PySpark & Databricks – File I/O, Tables, JSON Handling, Nested Data, Parse Modes, Permissions

---

## 🔹 1. Ways to Read Data in PySpark

PySpark allows reading data **directly from paths** or from **registered tables/views**.

### (a) Path-Based Reads (with `.option()` / `.options()`)

```python
df_csv = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                   .option("sep", ",") \
                   .csv("/path/file.csv")

df_json = spark.read.option("multiLine", True) \
                    .option("mode", "PERMISSIVE") \
                    .json("/path/file.json")

df_parquet = spark.read.option("mergeSchema", True) \
                        .parquet("/path/file.parquet")

df_orc = spark.read.orc("/path/file.orc")

df_text = spark.read.text("/path/file.txt")
```

✅ `.option()` → set single configuration.
✅ `.options()` → set multiple configurations at once.
Example:

```python
df_csv = spark.read.options(header=True, inferSchema=True, sep=",") \
                   .csv("/path/file.csv")
```

---

### (b) Table-Based Reads

```python
df1 = spark.read.table("my_db.my_table")   # via Spark read API
df2 = spark.table("my_db.my_table")        # shorthand
df3 = spark.sql("SELECT * FROM my_db.my_table")  # SQL
```

✅ Use when table is **registered in Hive/Unity Catalog metastore**.

---

### (c) Temporary Views

```python
df.createOrReplaceTempView("temp_view")
df_temp = spark.table("temp_view")  # session-scoped

df.createGlobalTempView("global_view")
df_glob = spark.table("global_temp.global_view")  # cluster-wide
```

✅ Useful when you want to run **SQL on DataFrames** without saving to disk.

---

## 🔹 2. File Formats Supported (with `.option()`)

| Format            | Read Example                                      | Write Example                  | Features                            | Best Use Case         |
| ----------------- | ------------------------------------------------- | ------------------------------ | ----------------------------------- | --------------------- |
| **CSV**           | `.option("header",True).csv()`                    | `.option("header",True).csv()` | Simple, human-readable, flexible    | Small data, exports   |
| **JSON**          | `.option("multiLine",True).json()`                | `.json()`                      | Handles nested/array data           | API logs, events      |
| **Parquet**       | `.option("mergeSchema",True).parquet()`           | `.parquet()`                   | Columnar, compressed, fast          | Analytics queries     |
| **ORC**           | `.orc()`                                          | `.orc()`                       | Hive-optimized                      | Hadoop/Hive workloads |
| **Avro**          | `.format("avro").load()`                          | `.format("avro").save()`       | Schema evolution support            | Streaming, Kafka      |
| **Text**          | `.text()`                                         | `.text()`                      | Line-based                          | Logs, plain text      |
| **Delta Lake** 🟢 | `.format("delta").option("versionAsOf",3).load()` | `.format("delta").save()`      | ACID, schema evolution, time travel | Databricks Lakehouse  |

---

## 🔹 3. Handling JSON & Nested Data (with `.option()`)

```python
df = spark.read.option("multiLine", True) \
               .option("mode", "PERMISSIVE") \
               .json("/path/data.json")
```

---

### (a) Load Nested JSON

`.option("multiLine", True)` → required when JSON spans multiple lines.

---

### (b) Access Nested Fields

```python
df.select("id", "address.city", "address.zip").show()
```

---

### (c) Explode Arrays

```python
from pyspark.sql.functions import explode
df.select("id", explode("phones").alias("phone")).show()
```

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

df = spark.read.schema(schema) \
               .option("multiLine", True) \
               .json("/path/data.json")
```

---

### (e) Flatten Nested JSON

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

---

## 🔹 4. Parse Modes & Options

### JSON & CSV Parse Modes

```python
df = spark.read.option("mode", "DROPMALFORMED").json("/path/file.json")
```

| Mode              | Behavior                                          |
| ----------------- | ------------------------------------------------- |
| **PERMISSIVE**    | Keeps corrupt records in `_corrupt_record` column |
| **DROPMALFORMED** | Skips malformed rows                              |
| **FAILFAST**      | Stops execution if any malformed row found        |

---

### CSV Extra Options

```python
df = spark.read.option("header", True) \
               .option("sep", "|") \
               .option("inferSchema", True) \
               .csv("/path/data.csv")
```

---

### JSON Extra Options

```python
df = spark.read.option("multiLine", True) \
               .option("allowSingleQuotes", True) \
               .option("allowUnquotedFieldNames", True) \
               .option("mode", "FAILFAST") \
               .json("/path/data.json")
```

---

### Parquet & ORC Options

```python
df = spark.read.option("mergeSchema", True).parquet("/path/data.parquet")
```

---

### Delta Lake Options

```python
df = spark.read.format("delta") \
               .option("versionAsOf", 3) \
               .load("/mnt/delta/events")
```

---

💡 **Summary:**

* `.option()` / `.options()` make reads/writes flexible & configurable.
* Use **path-based** reads for direct files, **table-based** reads for Hive/Unity Catalog tables, and **views** for temporary SQL access.
* Know **different file formats**, their options, and **parse modes**.
* For **nested JSON**, use `.option("multiLine", True)` and schema definitions.
* For **Delta Lake**, use `.format("delta")` with time travel options.

---
