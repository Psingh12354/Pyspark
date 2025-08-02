# PySpark Detailed Notes \& Best Practices

## 🔥 What is PySpark?

**PySpark** is the Python API for Apache Spark, enabling distributed big data processing directly from Python. While Spark is written in Scala, PySpark leverages the **Py4j** library for interoperability so you can use Python instead of Scala or Java.

- Combines Python's simplicity with Spark's power
- Highly scalable for petabyte-scale data
- **100x faster** than Hadoop MapReduce
- Keeps processing in **RAM** for top performance


## 🚀 Getting Started with PySpark

### Installation

```bash
pip install pyspark
```


### Initialize a Spark Session

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()
```


### Creating an RDD

```python
data = ["Spark", "is", "awesome"]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
```


## ⚙️ Databricks Cluster Essentials

- **Databricks Cluster:** Bundle of compute resources for running Spark jobs/notebooks.
    - **All-purpose Cluster:** For collaborative explorations and notebooks.
    - **Job Cluster:** For running and terminating after jobs—cost-efficient.


## 🖥️ Driver Node vs Worker Node

| Feature | Driver Node | Worker Node |
| :-- | :-- | :-- |
| Function | Runs main Spark logic, schedules tasks | Executes tasks in parallel |
| Storage | Maintains job metadata/state | Handles data read/write |

## 🔄 RDD Operations

### Transformations (Lazy, Build a DAG)

- `map()`: Apply function to each element
- `flatMap()`: Like `map`, but flattens results
- `filter()`: Keeps elements matching a condition
- `groupBy()`: Groups elements by key

```python
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
squared_rdd = rdd.map(lambda x: x*x)
print(squared_rdd.collect())  # [1, 4, 9, 16, 25]
```


### Actions (Trigger Execution)

- `count()`: Number of elements
- `collect()`: All elements as a list
- `take(n)`: First n elements


## 🏗️ Window Functions vs GroupBy

| Aspect | Window Functions | GroupBy Aggregates |
| :-- | :-- | :-- |
| Use | Row-based calc (ranking, moving avg) | Aggregation (sum, avg, count) |
| Scope | Subset (window) within group | Entire group |
| Example | `ROW_NUMBER()`, `LAG()`, `LEAD()` | `SUM()`, `AVG()`, `COUNT()` |

## 📊 Spark Ecosystem Components

- **Spark Core:** Distributed task scheduling
- **Spark SQL:** Structured data \& DataFrame API (SQL-like)
- **Spark Streaming:** Real-time streaming analytics
- **MLlib:** Machine learning algorithms
- **GraphX:** Graph analytics


## 🔄 Data Ingestion Modes

| Mode | Description |
| :-- | :-- |
| Batch Processing | Data in fixed chunks (large, periodic uploads) |
| Real-time | Process as data arrives (live dashboards, alerts) |

## ⚡ Building An ETL Pipeline in PySpark

An end-to-end **Extract → Transform → Load** (ETL) workflow:

```python
df = spark.read.csv("input.csv", header=True)
df = df.withColumn("new_col", df["existing_col"] * 10)
df.write.format("parquet").save("output.parquet")
```


## 🏗️ Data Lake vs Data Warehouse

| Feature | Data Warehouse | Data Lake |
| :-- | :-- | :-- |
| Data Type | Structured | All formats (structured/unstructured) |
| Processing | Batch | Batch \& Streaming |
| Use Case | Analytics/BI | Data science, ML, analytics |

## 🏢 Data Warehouse vs Data Mart

| Feature | Data Warehouse | Data Mart |
| :-- | :-- | :-- |
| Scope | Entire enterprise | Project/department |
| Data Volume | Large | Smaller |
| Focus | Aggregate analytics | Departmental reporting |

## 🔄 Delta Lake vs Data Lake

| Feature | Delta Lake | Data Lake |
| :-- | :-- | :-- |
| ACID Support | Yes | No |
| Schema Enforcement | Yes | No |
| Metadata Mgmt | Advanced | Basic |

## 🔗 Data Integration

Bringing data together from various sources for a unified analytics view (joins, merges, pipeline design).

## 📝 Delta Lake Version Control

Manage time travel and rollbacks:

```sql
DESCRIBE HISTORY employee1;
SELECT * FROM employee1@v1;
RESTORE TABLE employee1 TO VERSION AS OF 1;
```


## 📑 Views in Spark

- **Temporary View:** Within current session
- **Global Temp View:** Accessible across all Spark sessions

```python
df.createOrReplaceTempView("temp_view")
df.createGlobalTempView("global_view")
```


## 🔍 Window Functions in PySpark

Example: Assigning row numbers by department and salary order.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("department").orderBy("salary")
df = df.withColumn("row_number", row_number().over(window_spec))
```


## 📌 Advanced PySpark Topics

### User Defined Functions (UDFs)

Apply custom Python logic to DataFrame columns.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def custom_function(value):
    return value.upper()

uppercase_udf = udf(custom_function, StringType())
df = df.withColumn("uppercase_column", uppercase_udf(df["existing_column"]))
```


### Handling Missing Values

```python
df = df.na.fill({"age": 0, "name": "Unknown"})  # Fill missing values
df = df.na.drop()  # Drop rows with any nulls
```


### DataFrame Joins

```python
df1.join(df2, df1.id == df2.id, "inner").show()
```


# 🔍 PySpark `when` and `otherwise` – Conditional Logic

## Overview

Mimics SQL's `CASE WHEN` for conditional transformations.

- Use `when()` and `otherwise()` from `pyspark.sql.functions`
- More efficient than UDFs for such logic


### Syntax

```python
from pyspark.sql.functions import when, col

df = df.withColumn(
    "age_group",
    when(col("age") < 18, "Minor")
    .when((col("age") >= 18) & (col("age") < 60), "Adult")
    .otherwise("Senior")
)
```


### Chaining Conditions

```python
when(cond1, val1).when(cond2, val2).otherwise(default)
```


### Sample Output

| name | age | age_group |
| :-- | :-- | :-- |
| Alice | 17 | Minor |
| Bob | 25 | Adult |
| Cathy | 62 | Senior |

### Best Practices

- Use `when` over UDF for better speed and optimization
- Reference columns inside `when` with `col()`
- Order matters: first match applies

---

## Catalyst Optimizer vs. AQE in Spark

### Catalyst Optimizer

- Spark’s static (pre-execution) query optimizer.
- Makes the first, most efficient plan before running the query.
- Uses rules and estimated stats to:
    - Push filters down
    - Remove unused columns
    - Reorder joins for speed


### Adaptive Query Execution (AQE)

- Introduced in Spark 3.0. Runs during query execution.
- Improves the plan as data is processed (runtime).
- Can:
    - Change join type (e.g., sort-merge to broadcast)
    - Merge small partitions or split big ones
    - Fix skewed data issues


### Key Differences

| Catalyst | AQE |
| :-- | :-- |
| Before execution | During execution |
| Uses static stats | Uses real-time stats |
| Makes initial plan | Adjusts plan as runs |

### How do they work together?

- Catalyst makes the first good plan.
- AQE tweaks and improves it as the job runs based on real data.


### Why use both?

- Catalyst is good for most queries.
- AQE is great for big or unpredictable data where things might change as the query runs.

**Remember:**
Catalyst = BEFORE running (static).
AQE = WHILE running (dynamic).
Both aim to make Spark SQL run as fast as possible.

---

## 🦾 Additional PySpark Features \& Best Practices

- **Broadcast Joins:** Use `.broadcast()` for small tables to optimize joins
- **Cache/Persist:** Use `.cache()` or `.persist()` to reuse DataFrames/RDDs in memory
- **Partitioning:** Repartition with `.repartition()` or `.coalesce()` for performance tuning
- **Spark SQL Optimization:** Use `explain()` to view and optimize execution plans
- **Reading Different Formats:** Supports CSV, Parquet, Avro, ORC, JSON, Delta, etc.
- **Logging:** Use Spark's built-in logger to trace issues
- **Job Monitoring:** Monitor runs via Spark UI or Databricks UI


## 🏁 Conclusion

PySpark empowers you to build robust, highly scalable data pipelines with Python and distributed Spark compute. Mastering its API, DataFrames, RDDs, SQL features, and best practices equips you for large-scale data engineering, analytics, and machine learning.

Keep exploring: window functions, performance tuning, streaming, advanced joins, and integrations with cloud platforms for real-world, production ETL and analytics!

