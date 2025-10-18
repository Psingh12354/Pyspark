## 🧠 **Understanding Date Operations in PySpark**

In data engineering, **date and time columns** are crucial for analytics, scheduling, trend analysis, and ETL transformations.
PySpark (via `pyspark.sql.functions`) provides **built-in functions** for date manipulation and comparison.

---

### ⚙️ **Commonly Used PySpark Date Functions**

| Category        | Function                                 | Description                          | Example                                  |
| --------------- | ---------------------------------------- | ------------------------------------ | ---------------------------------------- |
| 📅 Conversion   | `to_date()`                              | Converts string → DateType           | `to_date("2025-10-18", "yyyy-MM-dd")`    |
| ⏱️ Extraction   | `year()`, `month()`, `dayofmonth()`      | Extracts year, month, or day         | `year(col("order_date"))`                |
| 🧮 Arithmetic   | `date_add()`, `date_sub()`, `datediff()` | Add/subtract days or find difference | `date_add("order_date", 5)`              |
| 🧭 Formatting   | `date_format()`                          | Formats date into readable text      | `date_format("order_date", "E")` → “Mon” |
| 🔍 Filtering    | Use `col()` with comparison              | Filter by range or specific date     | `col("date") >= "2025-01-01"`            |
| 🗓️ Week & Day  | `weekofyear()`, `dayofweek()`            | Get week number or day of week       | `weekofyear("2025-10-18")` → 42          |
| ⏳ Current Dates | `current_date()`, `current_timestamp()`  | Returns system date/time             | `current_date()`                         |

---

## 📋 **Common Date Format Patter**

| **Pattern**               | **Example Output**        | **Description**              |
| ------------------------- | ------------------------- | ---------------------------- |
| `yyyy-MM-dd`              | `2025-10-18`              | Year-Month-Day (ISO default) |
| `dd-MM-yyyy`              | `18-10-2025`              | Day-Month-Year               |
| `MM/dd/yyyy`              | `10/18/2025`              | U.S. style Month/Day/Year    |
| `yyyy/MM/dd`              | `2025/10/18`              | Alternate Year/Month/Day     |
| `yyyyMMdd`                | `20251018`                | Compact numeric format       |
| `dd-MMM-yyyy`             | `18-Oct-2025`             | Abbreviated month name       |
| `dd-MMMM-yyyy`            | `18-October-2025`         | Full month name              |
| `E`                       | `Sat`                     | Short day of week            |
| `EEEE`                    | `Saturday`                | Full day of week             |
| `HH:mm:ss`                | `14:45:32`                | 24-hour time                 |
| `hh:mm:ss a`              | `02:45:32 PM`             | 12-hour time with AM/PM      |
| `yyyy-MM-dd HH:mm:ss`     | `2025-10-18 14:45:32`     | Full date + time             |
| `yyyy-MM-dd'T'HH:mm:ss`   | `2025-10-18T14:45:32`     | ISO 8601-style               |
| `yyyy-MM-dd HH:mm:ss.SSS` | `2025-10-18 14:45:32.123` | Date + milliseconds          |
| `w`                       | `42`                      | Week number in year          |
| `D`                       | `291`                     | Day number in year           |
| `Q`                       | `4`                       | Quarter (1–4)                |
| `YYYY-‘Q’Q`               | `2025-Q4`                 | Year + quarter label         |

---
## 🧩 **Important Notes About Date Columns**

1. **DateType vs StringType**

   * PySpark treats `"2025-10-18"` as a string unless explicitly converted using `to_date()`.
   * Always convert to `DateType` before doing date math or filtering.

2. **Date Format Handling**

   * You can specify custom formats using:

     ```python
     to_date(col("date_str"), "dd-MM-yyyy")
     ```
   * If you don’t specify, Spark assumes `yyyy-MM-dd`.

3. **Timezone Awareness**

   * `current_timestamp()` gives time in **UTC**.
   * You can convert using `from_utc_timestamp()` or `to_utc_timestamp()`.

4. **Nulls in Invalid Dates**

   * If the format doesn’t match, Spark returns `null` instead of an error.

     ```python
     to_date(lit("18/10/2025"), "yyyy-MM-dd") → null
     ```

---

Now that you have the background, let’s move to **real problems + solutions** 👇

---

## 🧮 **1️⃣ Extract Year, Month, and Day from a Date Column**

**Scenario:**
Your dataset contains an `order_date` column (e.g., `"2025-10-18"`).
You want to break it down into **year**, **month**, and **day** for analysis.

**Solution:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession.builder.appName("DateOps").getOrCreate()

data = [("2024-05-17",), ("2025-02-10",), ("2025-10-18",)]
df = spark.createDataFrame(data, ["order_date"])

df = df.withColumn("year", year("order_date")) \
       .withColumn("month", month("order_date")) \
       .withColumn("day", dayofmonth("order_date"))

df.show()
```

**Explanation:**

* `year(col)` → extracts the year portion
* `month(col)` → extracts month number
* `dayofmonth(col)` → gives day number (1–31)

---

## 📏 **2️⃣ Find the Number of Days Between Two Dates**

**Scenario:**
You need to calculate **delivery delay** by comparing `order_date` and `delivery_date`.

**Solution:**

```python
from pyspark.sql.functions import datediff

data = [("2025-10-01", "2025-10-18"),
        ("2025-09-10", "2025-09-25")]
df = spark.createDataFrame(data, ["order_date", "delivery_date"])

df = df.withColumn("days_diff", datediff("delivery_date", "order_date"))
df.show()
```

**Explanation:**

* `datediff(end_date, start_date)` returns an **integer** showing the number of days.
* Negative values mean the second date is earlier.

---

## ➕ **3️⃣ Add or Subtract Days**

**Scenario:**
For every order, you need to find:

* A **follow-up date** (7 days after order)
* A **reminder date** (3 days before order)

**Solution:**

```python
from pyspark.sql.functions import date_add, date_sub

data = [("2025-10-10",), ("2025-10-18",)]
df = spark.createDataFrame(data, ["order_date"])

df = df.withColumn("followup_date", date_add("order_date", 7)) \
       .withColumn("reminder_date", date_sub("order_date", 3))

df.show()
```

**Explanation:**

* `date_add()` → adds days
* `date_sub()` → subtracts days
* These are useful in **SLA** or **expiry date** calculations.

---

## 🔎 **4️⃣ Filter by Specific Month or Date Range**

**Scenario:**
You only want orders placed in **October 2025**.

**Solution:**

```python
from pyspark.sql.functions import col

data = [("2025-09-28",), ("2025-10-05",), ("2025-10-15",), ("2025-11-02",)]
df = spark.createDataFrame(data, ["order_date"])

filtered_df = df.filter((col("order_date") >= "2025-10-01") &
                        (col("order_date") <= "2025-10-31"))

filtered_df.show()
```

**Explanation:**

* Date comparison works because Spark stores `DateType` in ISO format.
* Ideal for **monthly reporting** and **period-based filtering**.

---

## 🗓️ **5️⃣ Convert String to DateType**

**Scenario:**
Your column is stored as `"18-10-2025"` (string), but you want a true date column.

**Solution:**

```python
from pyspark.sql.functions import to_date

data = [("18-10-2025",), ("09-02-2024",)]
df = spark.createDataFrame(data, ["order_date_str"])

df = df.withColumn("order_date", to_date("order_date_str", "dd-MM-yyyy"))
df.show()
```

**Explanation:**

* Always specify the format that matches your string.
* Once converted, you can safely use date operations like `datediff`, `date_add`, etc.

---

## 🧭 **6️⃣ Get Day of Week or Week Number**

**Scenario:**
You want to find which **weekday** an order was placed on and its **week number**.

**Solution:**

```python
from pyspark.sql.functions import date_format, weekofyear

data = [("2025-10-18",), ("2025-10-20",)]
df = spark.createDataFrame(data, ["order_date"])

df = df.withColumn("day_of_week", date_format("order_date", "E")) \
       .withColumn("week_number", weekofyear("order_date"))

df.show()
```

**Explanation:**

* `date_format("E")` → short weekday name (`Mon`, `Tue`, etc.)
* `weekofyear()` → ISO week number (1–52)

---

## ⚡ Bonus Tips

| Use Case                    | Function                                                     |
| --------------------------- | ------------------------------------------------------------ |
| Get current date            | `current_date()`                                             |
| Get current timestamp       | `current_timestamp()`                                        |
| Convert string to timestamp | `to_timestamp("2025-10-18 14:30:00", "yyyy-MM-dd HH:mm:ss")` |
| Extract hour/min/sec        | `hour()`, `minute()`, `second()`                             |
| Round to month start        | `trunc("date", "MM")`                                        |
| Round to year start         | `trunc("date", "YYYY")`                                      |

---
