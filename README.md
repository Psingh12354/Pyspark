# PySpark

Apache Spark is written in Scala programming language. To support Python with Spark, Apache Spark community released a tool, **PySpark**. Using PySpark, you can work with RDDs in Python programming language also. It is because of a library called **Py4j** that they are able to achieve this.

### Some key points
- PySpark is the combo of Python and Spark
- Scalable
- **100x faster** than Hadoop MapReduce
- **10x faster** on disk
- Uses **RAM instead of local drive**, which increases processing speed

## 🚀 Getting Started with PySpark
### 🔹 Installation
To install PySpark, run the following command:
```bash
pip install pyspark
```

### 🔹 Initialize SparkSession
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()
```

### 🔹 Creating an RDD
```python
data = ["Spark", "is", "awesome"]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
```

## ⚙️ Databricks Cluster
A **Databricks Cluster** is a combination of computation resources and configurations on which you can run jobs and notebooks.

### 🔹 Types of Databricks Clusters
- **All-purpose Clusters:** Used for collaborative analysis in notebooks.
- **Job Clusters:** Created for running automated jobs and terminated after execution.

## 🖥️ Driver Node vs Worker Node
| Feature  | Driver Node | Worker Node |
|----------|------------|-------------|
| Function | Runs the main function and schedules tasks. | Executes tasks assigned by the driver. |
| Storage  | Stores metadata, application states. | Reads/Writes from data sources. |

## 🔄 RDD Operations
### 🔹 Transformations
- **map()** – Applies function to each element
- **flatMap()** – Similar to map, but flattens results
- **filter()** – Filters elements based on condition
- **groupBy()** – Groups elements based on key

**Example:**
```python
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
squared_rdd = rdd.map(lambda x: x*x)
print(squared_rdd.collect())  # Output: [1, 4, 9, 16, 25]
```

### 🔹 Actions
- **count()** – Returns number of elements
- **collect()** – Returns all elements
- **take(n)** – Returns first `n` elements

## 🏗️ Windows vs GroupBy
| Feature | Windows | GroupBy |
|---------|---------|---------|
| Purpose | Used for row-based calculations, like ranking and moving averages. | Used for aggregations on groups of data. |
| Scope | Works on a subset (window) of data within a group. | Works on entire groups of data. |
| Example | ROW_NUMBER(), LAG(), LEAD() | SUM(), COUNT(), AVG() |

## 📊 Spark Components
- **Spark Core:** Core engine for distributed computing.
- **Spark SQL:** Structured data processing using SQL.
- **Spark Streaming:** Real-time data processing.
- **MLlib:** Machine learning library.
- **GraphX:** Graph computations.

## 🔄 Data Ingestion
| Type | Description |
|------|-------------|
| **Batch Processing** | Collects and processes data in groups. Good for large datasets. |
| **Real-time Processing** | Processes data as it arrives. Used in live analytics. |

## ⚡ ETL Pipeline
A data pipeline performing **Extract, Transform, Load** operations.
**Example:**
```python
df = spark.read.csv("input.csv", header=True)
df = df.withColumn("new_col", df["existing_col"] * 10)
df.write.format("parquet").save("output.parquet")
```

## 🏗️ Data Warehouse vs Data Lake
| Feature | Data Warehouse | Data Lake |
|---------|---------------|-----------|
| Data Type | Structured | Structured, Semi-structured, Unstructured |
| Processing | Batch Processing | Batch & Real-time Processing |

## 🏢 Data Warehouse vs Data Mart
| Feature | Data Warehouse | Data Mart |
|---------|---------------|-----------|
| Scope | Enterprise-wide | Specific project or department |
| Data Size | Large | Small |
| Usage | Aggregated data for analytics | Department-specific data |

## 🔄 Delta Lake vs Data Lake
| Feature | Delta Lake | Data Lake |
|---------|-----------|-----------|
| ACID Transactions | Yes | No |
| Schema Enforcement | Yes | No |
| Metadata Handling | Advanced | Basic |

## 🔗 Data Integration
The process of combining data from multiple sources into a single, unified view for analytics and decision-making.

## 🔄 Version Control in Delta Lake
### 🔹 Restore previous data version
```sql
DESCRIBE HISTORY employee1;  -- List versions
SELECT * FROM employee1@v1;  -- View version 1
RESTORE TABLE employee1 TO VERSION AS OF 1;  -- Restore version 1
```

## 📑 View in Spark
A **view** is a read-only logical table based on the result set of a query.
- **Temporary View** – Exists only in the current session.
- **Global View** – Exists across multiple sessions.

**Example:**
```python
df.createOrReplaceTempView("temp_view")
df.createGlobalTempView("global_view")
```

## 🔍 Window Functions in PySpark
### 🔹 Example of Row Number
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("department").orderBy("salary")
df = df.withColumn("row_number", row_number().over(window_spec))
```

## 📌 Additional Topics
### 🔹 PySpark UDFs (User Defined Functions)
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def custom_function(value):
    return value.upper()

uppercase_udf = udf(custom_function, StringType())
df = df.withColumn("uppercase_column", uppercase_udf(df["existing_column"]))
```

### 🔹 Handling Missing Values
```python
df = df.na.fill({"age": 0, "name": "Unknown"})
df = df.na.drop()
```

### 🔹 Joining DataFrames
```python
df1.join(df2, df1.id == df2.id, "inner").show()
```

# 🔍 PySpark `when` and `otherwise` – Conditional Logic in DataFrames

## 📘 Overview

In PySpark, conditional logic (similar to SQL's `CASE WHEN`) is implemented using the `when()` and `otherwise()` functions from `pyspark.sql.functions`.

These functions are **Catalyst-optimized**, meaning they are faster and more efficient than using UDFs for conditional expressions.

---

## 📦 Import Required Functions

You need to import from `pyspark.sql.functions`:

```
from pyspark.sql.functions import when, col
```

---

## 🧠 Syntax

```
when(condition, value).otherwise(default_value)
```

- `condition`: A boolean expression (e.g. `col("age") > 18`)
- `value`: Value to assign if the condition is true
- `otherwise`: Fallback value if no `when` conditions match

> ✅ You can **chain multiple `when()` clauses** for multiple conditions (like `if-elif-else` in Python).

---

## 🛠️ Use Case Example

### 🎯 Goal: Categorize people based on age

| Age  | Category |
|------|----------|
| <18  | Minor    |
| 18–59| Adult    |
| ≥60  | Senior   |

```
from pyspark.sql.functions import when, col

df = df.withColumn(
    "age_group",
    when(col("age") < 18, "Minor")
    .when((col("age") >= 18) & (col("age") < 60), "Adult")
    .otherwise("Senior")
)
```

---

## 📊 Sample Output

| name  | age | age_group |
|--------|-----|-----------|
| Alice  | 17  | Minor     |
| Bob    | 25  | Adult     |
| Cathy  | 62  | Senior    |

---

## 🧪 Multiple Conditions with `when()`

You can chain `when()` like this:

```
when(condition1, value1)
 .when(condition2, value2)
 .otherwise(default_value)
```

---

## ✅ Best Practices

- Prefer `when`/`otherwise` over UDFs for performance.
- Always use `col()` when referencing DataFrame columns inside conditions.
- Conditions are evaluated in order — the **first match wins**.

---

## 📌 Conclusion
PySpark is a powerful tool for distributed computing. Understanding its core concepts, RDD operations, Spark SQL, MLlib, and streaming capabilities enables efficient data processing at scale.
