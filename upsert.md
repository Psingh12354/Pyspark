## 🧩 What is an Upsert?

**Upsert = Update + Insert**

In Delta Lake, **upsert** means:

* If a record **already exists** (based on a matching key), we **update** it.
* If a record **does not exist**, we **insert** it.

In SQL terms → it’s similar to `MERGE INTO`.

---

## ✅ PySpark Delta Lake Upsert Logic Example

Let’s say we have:

* A **target Delta table** (the main data).
* A **source DataFrame** (new incoming data, maybe from CDC or incremental load).

---

### 🗂️ Example Data

#### Target table (`targetDF`)

| id | name  | city    |
| -- | ----- | ------- |
| 1  | Rahul | Delhi   |
| 2  | Neha  | Mumbai  |
| 3  | Arjun | Chennai |

#### Source data (`sourceDF`)

| id | name | city      |
| -- | ---- | --------- |
| 2  | Neha | Pune      |
| 4  | Riya | Bangalore |

---

### ⚙️ Step 1: Write initial target table

```python
from delta.tables import DeltaTable

# Example target data
targetDF = spark.createDataFrame([
    (1, "Rahul", "Delhi"),
    (2, "Neha", "Mumbai"),
    (3, "Arjun", "Chennai")
], ["id", "name", "city"])

target_path = "/mnt/delta/target_table"
targetDF.write.format("delta").mode("overwrite").save(target_path)
```

---

### ⚙️ Step 2: Create the incoming source data

```python
sourceDF = spark.createDataFrame([
    (2, "Neha", "Pune"),        # existing record → should update city
    (4, "Riya", "Bangalore")    # new record → should insert
], ["id", "name", "city"])
```

---

### ⚙️ Step 3: Perform UPSERT using DeltaTable `merge`

```python
# Load Delta table
deltaTable = DeltaTable.forPath(spark, target_path)

# Merge logic
(
    deltaTable.alias("t")
    .merge(
        sourceDF.alias("s"),
        "t.id = s.id"   # Matching condition (Primary Key or Business Key)
    )
    .whenMatchedUpdate(set={
        "name": "s.name",
        "city": "s.city"
    })
    .whenNotMatchedInsert(values={
        "id": "s.id",
        "name": "s.name",
        "city": "s.city"
    })
    .execute()
)
```

---

### ⚙️ Step 4: Verify the result

```python
deltaTable.toDF().show()
```

✅ **Output:**

| id | name  | city      |            |
| -- | ----- | --------- | ---------- |
| 1  | Rahul | Delhi     |            |
| 2  | Neha  | Pune      | ← updated  |
| 3  | Arjun | Chennai   |            |
| 4  | Riya  | Bangalore | ← inserted |

---

## 🧠 Key Points to Remember

| Concept                      | Meaning                                          |
| ---------------------------- | ------------------------------------------------ |
| **`merge()`**                | Used for upsert logic in Delta Lake              |
| **`whenMatchedUpdate()`**    | Runs when record exists in target                |
| **`whenNotMatchedInsert()`** | Runs when record not found in target             |
| **Condition**                | Typically based on primary key or business key   |
| **Delta Lake handles ACID**  | So multiple jobs can safely update concurrently  |
| **Schema evolution**         | Can be handled with `mergeSchema=True` if needed |

---

## 🧩 Optional: Upsert using SQL (in Databricks notebook)

```sql
MERGE INTO target_table t
USING source_table s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET t.city = s.city, t.name = s.name
WHEN NOT MATCHED THEN
  INSERT (id, name, city)
  VALUES (s.id, s.name, s.city);
```
