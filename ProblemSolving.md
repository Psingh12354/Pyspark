# **🔥 PySpark Practice Problems – Full Set of 10**

---

## **1️⃣ Top 2 Highest Paid Employees per Department**

**Problem Statement:**
Find the **top 2 highest-paid employees** in each department using a window function.

**Sample Data:**

```python
data = [("HR","Alice",50000),
        ("HR","Bob",60000),
        ("IT","Charlie",80000),
        ("IT","David",75000),
        ("IT","Emma",90000)]
columns = ["dept","emp_name","salary"]
df = spark.createDataFrame(data,columns)
```

**Solution:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

winSpec = Window.partitionBy("dept").orderBy(col("salary").desc())
df_top2 = df.withColumn("rank", rank().over(winSpec)) \
            .filter(col("rank") <= 2)
df_top2.show()
```

**Explanation:**

* `Window.partitionBy("dept")` groups data by department.
* `rank().over(winSpec)` assigns ranks based on descending salary.
* `.filter(col("rank") <= 2)` keeps only the top 2 employees per dept.

---

## **2️⃣ Replace Null Salaries with Department Average**

**Problem Statement:**
Replace `null` salaries with the **average salary of that department**.

**Sample Data:**

```python
data = [("HR","Alice",50000),
        ("HR","Bob",None),
        ("IT","Charlie",80000),
        ("IT","David",None)]
columns = ["dept","emp_name","salary"]
df = spark.createDataFrame(data,columns)
```

**Solution (Window):**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, when

winSpec = Window.partitionBy("dept")
df_filled = df.withColumn("avg_salary", avg("salary").over(winSpec)) \
              .withColumn("salary", when(col("salary").isNull(), col("avg_salary"))
                                      .otherwise(col("salary"))) \
              .drop("avg_salary")
df_filled.show()
```

**Explanation:**

* Window calculates **department-wise average**.
* `when().otherwise()` replaces null values.
* Clean approach; no joins required.

**Alternate Solution (groupBy + join):**

```python
dept_avg = df.groupBy("dept").agg(avg("salary").alias("avg_salary"))
df_filled = df.join(dept_avg, on="dept", how="left") \
              .withColumn("salary", when(col("salary").isNull(), col("avg_salary"))
                                       .otherwise(col("salary"))) \
              .drop("avg_salary")
```

---

## **3️⃣ Explode Array Column**

**Problem Statement:**
Explode a `skills` array into multiple rows.

**Sample Data:**

```python
data = [("Alice", ["Python","SQL"]),
        ("Bob", ["Java","Scala","Spark"])]
columns = ["emp_name","skills"]
df = spark.createDataFrame(data, columns)
```

**Solution:**

```python
from pyspark.sql.functions import explode
df_exploded = df.withColumn("skill", explode(col("skills"))).drop("skills")
df_exploded.show()
```

**Explanation:**

* `explode()` creates one row per element in the array.
* `drop("skills")` removes the original array column.

---

## **4️⃣ Calculate Years of Experience**

**Problem Statement:**
Calculate years of experience from `join_date` to today.

**Sample Data:**

```python
data = [("Alice","2018-01-01"),
        ("Bob","2020-06-15"),
        ("Charlie","2015-03-10")]
columns = ["emp_name","join_date"]
df = spark.createDataFrame(data, columns) \
          .withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))
```

**Solution (approximate):**

```python
from pyspark.sql.functions import year, current_date

df = df.withColumn("years_of_experience", year(current_date()) - year(col("join_date")))
df.show()
```

**Solution (accurate using months_between):**

```python
from pyspark.sql.functions import months_between, floor

df = df.withColumn("years_of_experience", floor(months_between(current_date(), col("join_date"))/12))
```

**Explanation:**

* Approximate method ignores months/days.
* `months_between` gives exact experience in years.

---

## **5️⃣ Aggregate Sales per Region and Category**

**Problem Statement:**
Find total sales per `region` and `category`, sorted descending.

**Sample Data:**

```python
data = [("East","Electronics",1000),
        ("East","Furniture",500),
        ("West","Electronics",1200),
        ("East","Electronics",800),
        ("West","Furniture",400)]
columns = ["region","category","sales"]
df = spark.createDataFrame(data,columns)
```

**Solution:**

```python
from pyspark.sql.functions import sum

df_total_sales = df.groupBy("region","category") \
                   .agg(sum("sales").alias("total_sales")) \
                   .orderBy(col("total_sales").desc())
df_total_sales.show()
```

**Explanation:**

* `groupBy()` groups by region & category.
* `sum().alias()` creates a new column `total_sales`.
* `orderBy()` sorts descending.

---

## **6️⃣ Find Duplicate Records**

**Problem Statement:**
Identify duplicate rows in the DataFrame.

**Sample Data:**

```python
data = [("Alice","HR",50000),
        ("Bob","IT",60000),
        ("Alice","HR",50000),
        ("Charlie","IT",70000)]
columns = ["emp_name","dept","salary"]
df = spark.createDataFrame(data, columns)
```

**Solution (groupBy + count):**

```python
df.groupBy(df.columns).count().filter(col("count")>1).show()
```

**Solution (window row_number):**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

winSpec = Window.partitionBy(*df.columns).orderBy(df.columns[0])
df_result = df.withColumn("row_num", row_number().over(winSpec)) \
              .filter(col("row_num")>1)
df_result.show()
```

**Explanation:**

* First approach: aggregated duplicates count.
* Second: keeps all duplicate rows except the first, useful for **de-duplication**.

---

## **7️⃣ 3-Day Moving Sales Average**

**Problem Statement:**
Compute 3-day rolling sum of sales per store.

**Sample Data:**

```python
data = [("S1","2025-10-01",100),
        ("S1","2025-10-02",200),
        ("S1","2025-10-03",300),
        ("S1","2025-10-04",400)]
columns = ["store","date","sales"]
df = spark.createDataFrame(data,columns) \
          .withColumn("date", to_date(col("date"),"yyyy-MM-dd"))
```

**Solution:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

winSpec = Window.partitionBy("store").orderBy("date").rowsBetween(-2,0)
df_sales = df.withColumn("sales_3days", sum(col("sales")).over(winSpec))
df_sales.show()
```

**Explanation:**

* `rowsBetween(-2,0)` → current + 2 previous rows.
* `sum().over(winSpec)` computes rolling sum per window.

---

## **8️⃣ Categorize Employees by Salary**

**Problem Statement:**
Classify salaries: `<50k → Low`, `50k-80k → Medium`, `>=80k → High`.

**Sample Data:**

```python
data = [("Alice",45000),
        ("Bob",60000),
        ("Charlie",90000)]
columns = ["emp_name","salary"]
df_salary = spark.createDataFrame(data,columns)
```

**Solution:**

```python
from pyspark.sql.functions import when

df_salary = df_salary.withColumn("salary_category",
    when(col("salary")<50000,"Low")
    .when((col("salary")>=50000) & (col("salary")<80000),"Medium")
    .otherwise("High")
)
df_salary.show()
```

**Explanation:**

* `when()` is chained for multiple conditions.
* `otherwise()` handles the last category.

---

## **9️⃣ Flatten Nested JSON Column**

**Problem Statement:**
Flatten a nested JSON column `address` into separate columns.

**Sample Data:**

```python
data = [("Alice", '{"city":"NY","zip":10001}'),
        ("Bob", '{"city":"LA","zip":90001}')]
columns = ["emp_name","address"]
df_json = spark.createDataFrame(data, columns)
```

**Solution:**

```python
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([StructField("city",StringType(),True),
                     StructField("zip",IntegerType(),True)])
df_flat = df_json.withColumn("address", from_json(col("address"), schema)) \
                 .select(col("emp_name"), col("address.*"))
df_flat.show()
```

**Explanation:**

* `from_json()` parses JSON string to a struct.
* `select(col("address.*"))` flattens struct into columns.

---

## **🔟 Employees Joined in Last 6 Months**

**Problem Statement:**
Filter employees who joined in the last 6 months.

**Sample Data:**

```python
data = [("Alice","2025-05-01"),
        ("Bob","2024-12-
```


01"),
("Charlie","2025-08-10")]
columns = ["emp_name","join_date"]
df_join = spark.createDataFrame(data, columns) 
.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))

````

**Solution:**
```python
from pyspark.sql.functions import months_between, current_date

df_recent = df_join.filter(months_between(current_date(), col("join_date")) <= 6)
df_recent.show()
````

**Explanation:**

* `months_between(current_date(), join_date)` calculates months difference.
* Filter keeps only employees who joined in last 6 months.

---

Absolutely ✅ — following the same GitHub markdown style, here are **Problem 12** and **Problem 13** that you can append directly after Problem 11 in your `ProblemSolving.md` file.

---

## **11️⃣ Percentage Contribution of Each Category to Total Sales**

**Problem Statement:**
For each region, calculate the percentage contribution of each category’s sales to the total sales of that region.

**Sample Data:**

```python
data = [
    ("East", "Furniture", 2000),
    ("East", "Electronics", 3000),
    ("West", "Furniture", 4000),
    ("West", "Electronics", 6000),
    ("North", "Furniture", 1500),
    ("North", "Electronics", 2500)
]
columns = ["region", "category", "sales"]

df_sales = spark.createDataFrame(data, columns)
```

**Solution:**

```python
from pyspark.sql.functions import col, sum

# Step 1: Calculate total sales per region
df_total_sales = (
    df_sales.groupBy(col("region").alias("region1"))
    .agg(sum("sales").alias("sum_sales"))
)

# Step 2: Join back to original DataFrame
df_result = (
    df_sales.join(df_total_sales, df_sales.region == df_total_sales.region1, how="inner")
    .withColumn("percent_of_total", (col("sales") * 100) / col("sum_sales"))
    .drop("sum_sales", "region1")
    .dropDuplicates()
)

df_result.show()
```

**Explanation:**

* `groupBy("region")` aggregates total sales per region.
* Join allows calculation of each category’s percentage contribution.
* `percent_of_total = sales * 100 / total_sales`.
* `dropDuplicates()` ensures no repeated rows.

---

## **12️ Second Highest Salary per Department**

**Problem Statement:**
Find the employee with the second highest salary in each department. Skip departments with only one employee.

**Sample Data:**

```python
data = [
    (1, "Alice", "HR", 50000),
    (2, "Bob", "HR", 60000),
    (3, "Charlie", "IT", 90000),
    (4, "David", "IT", 85000),
    (5, "Eve", "Finance", 75000),
    (6, "Frank", "Finance", 70000)
]
columns = ["emp_id", "emp_name", "dept", "salary"]

df_salary = spark.createDataFrame(data, columns)
```

**Solution:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank

# Step 1: Define window partitioned by department, ordered by salary descending
winSpec = Window.partitionBy("dept").orderBy(col("salary").desc())

# Step 2: Rank employees and filter for rank 2
df_second_highest = df_salary.withColumn("rank", rank().over(winSpec)).filter(col("rank") == 2)

df_second_highest.show()
```

**Explanation:**

* `Window.partitionBy("dept")` ensures ranking happens within each department.
* `orderBy("salary").desc()` ranks employees by salary descending.
* `rank() == 2` picks the second highest salary.

---

✅ **Summary:**
These 10 problems cover:

1. Window functions (`rank`, `row_number`, `sum over window`)
2. Null handling & average imputation
3. Exploding arrays
4. Date calculations
5. Aggregations (`groupBy`, `agg`)
6. Categorization with `when`
7. Flattening JSON

