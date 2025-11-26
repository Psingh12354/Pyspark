# 🔥 Spark + Delta Lake + Joins Deep Interview Notes (30 REAL Scenario Q&A)

---

### Q1. You have two tables → 5M rows vs 2B rows. How do you decide join strategy?

Decision is NOT based on row count → based on **table size in bytes + memory availability**.

| Condition                              | Best Join Type              |
| -------------------------------------- | --------------------------- |
| Smaller table < 8GB + memory available | Broadcast Hash Join         |
| Broadcast fails or >8GB                | Shuffle Sort-Merge Join     |
| Key duplication is high                | Prefer Sort-Merge over Hash |
| Data skew exists                       | Salt keys, repartition, AQE |

Example:

```python
df = big_df.join(broadcast(small_df),"id")
```

If can't broadcast:

```python
df = big_df.repartition("id").join(small_df,"id")
```

---

### Q2. Same scenario — what if join key is highly duplicated?

When duplicate keys exist, **broadcast hash join struggles** due to large hash bucket expansion.

Best approach:

1. Use **Sort-Merge Join**, as sorting groups duplicates efficiently.
2. If skew too high → apply **salting**.
3. If only few keys are hot → isolate heavy keys.

Example salting:

```python
df = df.withColumn("salt", rand()*10)
df.join(other.withColumn("salt",rand()*10),["id","salt"])
```

---

### Q3. What if both datasets are large (200M vs 150M)? No table small enough for broadcast.

Use **Sort-Merge Join** with:

• repartition on join key
• filter pushdown before join
• drop unused columns before join

Example:

```python
df1 = df1.select("id","col1")
df2 = df2.select("id","col2")
df = df1.repartition("id").join(df2,"id")
```

---

### Q4. What if join key is skewed (e.g., 60% rows belong to one key)?

Fixes:

1. Salting
2. Split heavy key + union
3. Enable AQE dynamic skew optimization
4. Broadcast smaller table if possible

Example heavy key isolation:

```python
df_hot = big_df.filter("id='USA'")
df_rest = big_df.filter("id!='USA'")
final = df_hot.join(ref,"id").union(df_rest.join(ref,"id"))
```

---

### Q5. Explain Broadcast Join Threshold and real memory implications.

Default threshold:

```
spark.sql.autoBroadcastJoinThreshold = 8GB
```

But broadcast fails if:

• executor memory < broadcast size
• many cached DFs already using RAM
• wide rows → actual size > compressed file size

You always check:

```python
df_small._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
```

---

### Q6. How to decide join strategy in an interview instantly?

Use **this quick decision table**:

| Condition              | Join Type                |
| ---------------------- | ------------------------ |
| Small (<8GB) vs Huge   | Broadcast Hash Join      |
| Large vs Large         | Sort-Merge Join          |
| Key duplication high   | Sort-Merge Join          |
| Key skew               | Salting / Split Logic    |
| Memory low             | SMJ or Disk-persist hash |
| Real-time + small dims | Broadcast lookup cache   |

Memorize this — interview safe.

---

### Q7. Why Sort-Merge Join works better than Hash Join with duplicates?

Because duplicated keys = large hash buckets = memory spikes.
Sort-Merge groups identical keys contiguously → efficient nested join.

---

### Q8. If full broadcast is not possible, how to semi-optimize join?

Option → **partial broadcast / selective join**.

Filter dimension before broadcast:

```python
filtered = dim.filter("category='electronics'")
df.join(broadcast(filtered),"id")
```

---

### Q9. How do you reduce join time by pruning columns?

Join only required fields:

Wrong:

```python
df_large.join(dim,"id")
```

Correct:

```python
dim = dim.select("id","category")  # cut width
df=df_large.join(dim,"id")
```

Column width reduction improves serialization + shuffle cost.

---

### Q10. EMR/Databricks job slow after few months — join times growing. Why?

Not compute issue → **metadata + small files + fragmentation**.

Fix:

```
OPTIMIZE table ZORDER BY (id)
VACUUM 7
```

---

### Q11. Join huge fact with slowly changing dim. Best architecture?

Use **SCD Type-2 + Delta Lake Merge**, broadcast dimension if small.

---

### Q12. Fact table incremental load vs full join difference.

| Full Join             | Incremental Join          |
| --------------------- | ------------------------- |
| scans entire 2B rows  | scans only last partition |
| heavy                 | scalable                  |
| good for rare updates | necessary for daily loads |

---

### Q13. Why AQE reduces join time?

AQE enables:

1. dynamic broadcast
2. shuffle coalescing
3. skew handling auto-split

Enable:

```
spark.sql.adaptive.enabled=true
```

---

### Q14. What is Repartition vs Coalesce use in join optimization?

Repartition — increases partitions → parallelism (shuffle)
Coalesce — reduces partitions → fewer files

Joining big tables → **repartition by key**

```python
df.repartition(300,"id")
```

---

### Q15. What if driver memory crashes during join?

Cause → collect(), toPandas(), large broadcast.

Fix:

• never collect big DF
• broadcast only small
• move UDF → SQL expressions

---

### Q16. Why caching a table before join may increase speed?

Caching eliminates repeated shuffle for lookup-heavy joins.

But only cache if reused:

```python
dim.cache(); dim.count()
```

---

### Q17. When to use Bucket Join?

When joins happen repeatedly on same key — bucketed tables avoid shuffle.

Example:

```sql
CREATE TABLE fact CLUSTERED BY (id) INTO 256 BUCKETS;
```

---

### Q18. Explain cost difference: Broadcast Join vs SMJ vs Hash Join.

| Join           | Memory | Shuffle  | Best Use Case          |
| -------------- | ------ | -------- | ---------------------- |
| Broadcast Hash | High   | Very Low | Small dim joins        |
| Sort-Merge     | Medium | High     | Large-large joins      |
| Shuffle Hash   | High   | High     | Rarely chosen manually |

---

### Q19. How to pre-validate join selection before running?

Use:

```
df.explain("formatted")
```

Look for:
• Broadcast hash join
• Sort merge join
• Shuffle hash

---

### Q20. Final rulebook: When does each join win?

| Situation                   | Correct Join                        |
| --------------------------- | ----------------------------------- |
| 5M vs 2B — fits in memory   | Broadcast                           |
| 5M vs 2B — repeated keys    | Sort-Merge                          |
| Both >50M                   | Sort-Merge                          |
| Both skewed                 | Salt or split                       |
| Dimension reused repeatedly | Cache + Broadcast                   |
| Massive history table       | ZORDER + VACUUM + MERGE incremental |

---

### **Q21 — When should you use Shuffle-Hash Join (SAFL-H1)?**

Use Shuffle-Hash when:

| Condition               | Result                   |
| ----------------------- | ------------------------ |
| Both datasets large     | Broadcast not possible   |
| Sorting cost high       | SMJ slower               |
| Keys evenly distributed | Memory buckets efficient |

```python
df1.join(df2.hint("SHUFFLE_HASH"), "id")
```

---

### **Q22 — When is Shuffle-Hash Join NOT recommended?**

If key duplication or skew exists → Hash join creates huge hash buckets → OOM.

| Example Problem                              |
| -------------------------------------------- |
| 60% of rows have same key → bucket imbalance |

In this case **Sort-Merge Join** is safer.

```python
df1.repartition("id").join(df2,"id")
```

---

### **Q23 — Case: 200M vs 180M, no broadcast — which join?**

Sorting both → expensive
Hash join without sorting → cheaper

| Join Type    | Result               |
| ------------ | -------------------- |
| Broadcast    | No (too large)       |
| Sort-Merge   | Works but slow       |
| Shuffle-Hash | Best if keys uniform |

```python
df1.join(df2.hint("SHUFFLE_HASH"),"product_id")
```

---

### **Q24 — Dim table grows from 5GB → 12GB. Broadcast now failing.**

Earlier → Broadcast was ideal
Now > threshold → choose **Sort-Merge Join**

```python
big.repartition("cust_id").join(dim,"cust_id")
```

Broadcast = no longer viable.

---

### **Q25 — Real production case where Shuffle-Hash performs best**

E-commerce event log join:

| Table  | Size | Distribution |
| ------ | ---- | ------------ |
| logs   | 25GB | uniform      |
| clicks | 18GB | uniform      |

Sorting 43GB = slow → Shuffle-Hash faster by ~35%

```python
df_logs.join(clicks.hint("SHUFFLE_HASH"),"session_id")
```

---

### **Q26 — Broadcast Join Example with Real Sample Data**

### customer_dim (3.2M rows, 1.4GB)

| id  | name | segment |
| --- | ---- | ------- |
| 101 | John | Gold    |
| 102 | Tara | Bronze  |

### sales_txn (2.4B rows)

| id  | amount |
| --- | ------ |
| 101 | 1400   |
| 102 | 900    |

**Why Broadcast?**
Small → fits memory → avoids shuffling 2.4B rows.

```python
sales.join(broadcast(customers),"id")
```

---

### **Q27 — Sort-Merge due to high key duplicates (example included)**

### click_logs (skew)

| user_id | page  |
| ------- | ----- |
| 1001    | /home |
| 1001    | /cart |

### user_profile (duplicates)

| user_id | plan |
| ------- | ---- |
| 1001    | Gold |
| 1001    | Pro  |

Broadcast + Hash = risky
**Sort-Merge** handles duplication better.

```python
click_logs.repartition("user_id").join(user_profile,"user_id")
```

---

### **Q28 — Partition Example → Broadcast Recommended**

### sales table partitions

| month | rows |
| ----- | ---- |
| Jan   | 250M |
| Feb   | 240M |

customer_dim = 3.5M rows

```python
sales_2025.filter("month='Feb'") \
          .join(broadcast(customer_dim),"cust_id")
```

Only 1 partition scanned → best for **Broadcast Join**.

---

### **Q29 — Partition mismatch → Sort-Merge Join needed**

orders partitioned by `country`
join required on `user_id`

| Issue                                         |
| --------------------------------------------- |
| Partition key ≠ join key → shuffle inevitable |

Use SMJ + possible salting for skew:

```python
orders.repartition("user_id").join(users,"user_id")
```

---

### **Q30 — When to FORCE Shuffle-Hash over SMJ**

Both tables huge + equal spread keys:

| Condition                      | Pick                |
| ------------------------------ | ------------------- |
| No broadcast                   | ✔                   |
| SMJ too expensive (sorting TB) | ✔                   |
| Keys uniform                   | ✔ Shuffle-Hash wins |

```python
df1.join(df2.hint("SHUFFLE_HASH"),"id")
```

---

### **Q31 — Full Real Example With Two Sample Tables + Decision Outcome**

### Table A — partitioned by date (Fact)

| txn_id | cust_id | amount | date       |
| ------ | ------- | ------ | ---------- |
| 5001   | C101    | 900    | 2025-05-01 |
| 5002   | C101    | 300    | 2025-05-01 |
| 5003   | C102    | 1200   | 2025-05-01 |
| 5004   | C103    | 800    | 2025-05-02 |

### Table B — Customer Master (Dimension)

| cust_id | name  | city   |
| ------- | ----- | ------ |
| C101    | John  | Delhi  |
| C102    | Aditi | Mumbai |
| C103    | Ryan  | NYC    |

Scenario 1 → Only date='2025-05-01' required
→ only 33% partitions scanned
→ **Broadcast is ideal**

```python
fact.filter("date='2025-05-01'") \
    .join(broadcast(dim),"cust_id")
```

Scenario 2 → `cust_id` highly duplicated (C101 = 70% rows)
→ Broadcast may work BUT risk of bucket-explosion in hash
→ Better = **Sort-Merge Join + Salting**

```python
from pyspark.sql.functions import rand

fact = fact.withColumn("cid_salt", fact.cust_id + (rand()*10).cast("int"))
dim = dim.withColumn("cid_salt", dim.cust_id)

result = fact.join(dim,"cid_salt")
```

Now you can answer ANY join-based interview question confidently.

---

---

# **JOIN DECISION FLOW DIAGRAM (FINAL)**

```
                   Is one table small (< 8GB)?
                             │
              ┌──────────────┴──────────────┐
              │                             │
            YES                            NO
              │                             │
   BROADCAST HASH JOIN             Are tables very large?
   (Fastest, no shuffle)                    │
                                            │
                              ┌─────────────┴─────────────┐
                              │                           │
                            YES                          NO
                              │                           │
               Keys uniform + enough memory?       Skew/Duplicates present?
                              │                           │
                     ┌────────┴────────┐          ┌──────┴───────┐
                     │                 │          │              │
                 YES → SHUFFLE HASH    NO → SORT-MERGE JOIN      SALTING + SMJ
                (Avoid sorting cost)      (Stable for heavy dup)
```


