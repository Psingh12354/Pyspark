### Optimistic Concurrency Control (OCC) – Databricks Delta Lake

Optimistic Concurrency Control (OCC) is the mechanism used in Delta Lake (Databricks) to handle concurrent reads and writes without locking the data.

Instead of blocking operations using locks, OCC allows multiple transactions to proceed in parallel and validates conflicts only at the time of commit.

---

### How OCC Works

* Each transaction reads a **snapshot (version)** of the table
* Changes are performed in isolation
* Before committing, Delta Lake checks:

  * Whether underlying data has changed since the read
* If no conflict → commit succeeds
* If conflict → transaction fails and must retry

---

### Key Idea

> “Assume no conflict will happen, detect it only at commit time”

---

### Example Scenario

* Table version = 5
* Job A reads version 5
* Job B reads version 5

#### Case 1: No Conflict

* Job A writes new data → version becomes 6
* Job B writes different data (different files/partitions)
  → Both succeed

#### Case 2: Conflict

* Job A updates row X → version 6
* Job B also tries to update same row X
  → Job B fails due to conflict

---

### Types of Conflicts Detected

* Concurrent UPDATE on same data
* DELETE vs UPDATE on same data
* Overlapping file modifications
* Schema changes during transaction

---

### Internal Mechanism

* Delta Lake maintains a **transaction log (_delta_log)**
* Each commit creates a new version (JSON file)
* OCC checks:

  * Files read by transaction
  * Files modified by other transactions

---

### Code Example (PySpark)

```python
df = spark.read.format("delta").load("/mnt/delta/table")

# Transaction 1
df.filter("id = 1").write.format("delta").mode("overwrite").save("/mnt/delta/table")

# Transaction 2 (parallel)
df.filter("id = 1").write.format("delta").mode("overwrite").save("/mnt/delta/table")
```

👉 One of these will fail due to OCC conflict.

---

### Advantages of OCC

* No locking → better performance
* High concurrency support
* Scalable for distributed systems
* Works well with batch + streaming

---

### Limitations

* Transactions may fail → retry required
* Not suitable for heavy conflicting workloads
* Requires proper partitioning to reduce conflicts

---

### Best Practices

* Partition data properly (reduce overlap)
* Avoid frequent updates on same rows
* Use MERGE carefully in high concurrency scenarios
* Implement retry logic in pipelines

---

### OCC vs Pessimistic Concurrency Control

| Feature           | OCC            | Pessimistic       |
| ----------------- | -------------- | ----------------- |
| Locking           | No             | Yes               |
| Performance       | High           | Lower             |
| Conflict handling | At commit time | Prevented upfront |
| Scalability       | High           | Limited           |

---

### Short Interview Answer

Optimistic Concurrency Control in Delta Lake allows multiple transactions to run in parallel without locking data and detects conflicts at commit time using the transaction log. If a conflict occurs, the transaction fails and must be retried.
