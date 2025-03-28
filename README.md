# PySpark

Apache Spark is written in Scala programming language. To support Python with Spark, Apache Spark community released a tool, **PySpark**. Using PySpark, you can work with RDDs in Python programming language also. It is because of a library called **Py4j** that they are able to achieve this.

## 🔹 Key Features
- PySpark is a combination of Python and Apache Spark.
- Highly scalable and supports parallel processing.
- **100x** faster than Hadoop MapReduce.
- Uses **RAM instead of disk**, which significantly increases processing speed.
- Supports real-time data processing.

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

## 📊 Spark Components
### 🔹 Spark Streaming
Used for processing real-time streaming data.
**Example:**
```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([StructField("id", IntegerType(), True)])
df = spark.readStream.schema(schema).json("path/to/stream")
df.writeStream.format("console").start().awaitTermination()
```

### 🔹 MLlib
A machine learning library in Spark.
**Example:**
```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

# Sample data
data = [(0, 1.0, 2.0), (1, 2.0, 3.0)]
df = spark.createDataFrame(data, ["label", "feature1", "feature2"])
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df = assembler.transform(df)
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(df)
```

### 🔹 Spark SQL
Allows querying structured data using SQL.
**Example:**
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("table")
spark.sql("SELECT * FROM table WHERE age > 30").show()
```

## 🛠️ Data Ingestion
### 🔹 Batch Processing vs Real-time Processing
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
