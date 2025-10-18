# 🧠 **Databricks Cluster Types – Complete Notes**

---

## 🚀 **1️⃣ What is a Cluster in Databricks?**

A **cluster** in Databricks is a set of **compute resources (driver + worker nodes)** used to run **data processing workloads** such as:

* Notebooks
* Jobs
* SQL queries
* Streaming pipelines

Each cluster runs a **Databricks Runtime** (a Spark-based engine with optimizations).

---

## ⚙️ **2️⃣ Main Cluster Types in Databricks**

There are **3 primary cluster types** you’ll use most often:

| Cluster Type                              | Also Called         | Common Use Case                             |
| ----------------------------------------- | ------------------- | ------------------------------------------- |
| **All-Purpose Cluster**                   | Interactive Cluster | For development, ad-hoc analysis, notebooks |
| **Job Cluster**                           | Automated Cluster   | For production jobs, scheduled pipelines    |
| **SQL Warehouse (formerly SQL Endpoint)** | SQL Cluster         | For dashboards, BI, and SQL workloads       |

---

## 🧩 **3️⃣ Detailed Explanation**

### 🧠 **A. All-Purpose Cluster (Interactive Cluster)**

| Feature         | Description                                                                             |
| --------------- | --------------------------------------------------------------------------------------- |
| **Purpose**     | Used for **interactive data analysis**, experimentation, and collaborative development. |
| **Usage**       | Run notebooks manually (Python, SQL, R, Scala).                                         |
| **Runtime**     | Always-on (until manually stopped).                                                     |
| **Billing**     | **DBUs/hour** for the duration it’s running.                                            |
| **Ideal For**   | Data scientists, analysts, and developers.                                              |
| **Example Use** | Building and testing transformations before moving to production.                       |

**Pros:**

* Interactive
* Easy debugging
* Shared among users

**Cons:**

* Can be expensive if left running idle.

---

### ⚙️ **B. Job Cluster**

| Feature         | Description                                                                        |
| --------------- | ---------------------------------------------------------------------------------- |
| **Purpose**     | Used for **automated, production ETL jobs** or scheduled pipelines.                |
| **Usage**       | Created automatically when a Databricks job starts and destroyed when it finishes. |
| **Runtime**     | Temporary (auto-terminates).                                                       |
| **Billing**     | Only billed for **active runtime**.                                                |
| **Ideal For**   | Data engineers running scheduled batch jobs.                                       |
| **Example Use** | Nightly ETL pipeline writing Delta tables.                                         |

**Pros:**

* Cost-efficient
* Consistent environment per job
* No manual cluster management

**Cons:**

* Not interactive (no manual notebook execution)

---

### 🧮 **C. SQL Warehouse (SQL Cluster)**

| Feature         | Description                                                                                     |
| --------------- | ----------------------------------------------------------------------------------------------- |
| **Purpose**     | For **BI, dashboarding, and SQL queries**.                                                      |
| **Usage**       | Executes SQL statements in **Databricks SQL Editor** or connected BI tools (Power BI, Tableau). |
| **Runtime**     | Always available when queries are active.                                                       |
| **Billing**     | **Per DBU + VM usage** (SQL pricing tier).                                                      |
| **Ideal For**   | Analysts running queries and dashboards.                                                        |
| **Example Use** | Power BI dashboard connected to Databricks SQL Warehouse.                                       |

**Pros:**

* Auto-scaling
* Query caching
* Integration with BI tools

**Cons:**

* Less control over Spark configurations
* Limited to SQL workloads

---

## 💡 **4️⃣ Additional Cluster Variants**

While the above three are the main types, you’ll also see these specialized forms:

| Cluster Type                        | Description                                                                                                     |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **High Concurrency Cluster**        | A specialized **multi-user cluster** for serving concurrent queries securely. Used mostly for SQL/BI workloads. |
| **Photon Cluster**                  | Uses Databricks **Photon engine** for faster SQL execution (available for SQL & Delta workloads).               |
| **Single Node Cluster**             | Only one node (driver only). Useful for small tests, local processing, or lightweight ML tasks.                 |
| **Delta Live Tables (DLT) Cluster** | Created automatically by **DLT pipelines** to process streaming/batch ETL tasks.                                |

---

## 🧮 **5️⃣ Summary Comparison Table**

| Feature                | **All-Purpose Cluster**        | **Job Cluster**                    | **SQL Warehouse**         | **High Concurrency Cluster** | **DLT Cluster**          |
| ---------------------- | ------------------------------ | ---------------------------------- | ------------------------- | ---------------------------- | ------------------------ |
| **Purpose**            | Interactive analysis           | Automated ETL jobs                 | BI & SQL queries          | Multi-user concurrent SQL    | Data pipeline execution  |
| **Usage Mode**         | Manual (notebooks)             | Scheduled jobs                     | SQL editor / BI tools     | Shared SQL workloads         | DLT pipelines            |
| **Lifecycle**          | Persistent (manual start/stop) | Temporary (auto-created/destroyed) | Managed by Databricks SQL | Persistent                   | Managed by DLT           |
| **Cost Efficiency**    | Medium (can idle)              | High                               | Medium-High               | Medium                       | High                     |
| **DBU Rate**           | Standard DBU rate              | Lower DBU rate                     | SQL DBU rate              | Slightly higher              | DLT rate                 |
| **User Access**        | Interactive / shared           | Automated only                     | Query users               | Multi-tenant                 | Automated pipeline       |
| **Supports Notebooks** | ✅ Yes                          | ⚠️ Indirectly (via job)            | ❌ No                      | ⚠️ Limited                   | ❌ No                     |
| **Auto Termination**   | Optional                       | ✅ Yes                              | ✅ Yes                     | ✅ Yes                        | ✅ Yes                    |
| **Ideal Users**        | Data Scientists, Engineers     | Data Engineers                     | BI Analysts               | Multiple SQL users           | ETL Developers           |
| **Example**            | Ad-hoc analysis                | Nightly batch job                  | Power BI query            | Shared workspace             | Bronze → Silver pipeline |

---

## 🧩 **6️⃣ Choosing the Right Cluster Type**

| Scenario                           | Best Cluster                 |
| ---------------------------------- | ---------------------------- |
| Data exploration, notebook testing | **All-Purpose Cluster**      |
| Production ETL / batch job         | **Job Cluster**              |
| BI dashboard or SQL query          | **SQL Warehouse**            |
| Multi-user concurrent access       | **High Concurrency Cluster** |
| Automated pipeline (DLT)           | **DLT Cluster**              |

---

## 🧮 **7️⃣ Example Scenario**

Let’s say your company runs three workloads:

| Workload                | Cluster Type  | Runtime   | Example                       |
| ----------------------- | ------------- | --------- | ----------------------------- |
| Ad-hoc data exploration | All-purpose   | Manual    | Data scientist exploring CSVs |
| Nightly ETL job         | Job cluster   | 2 hrs     | Ingest + clean + write Delta  |
| Power BI dashboards     | SQL Warehouse | Always-on | Business team running SQL     |

Each of these clusters will have:

* Different **DBU consumption rates**
* Different **cost models**
* Different **user experiences**

---

## 🧠 **8️⃣ Quick Interview Answer**

> **Question:** What’s the difference between an All-Purpose Cluster, a Job Cluster, and a SQL Warehouse in Databricks?

✅ **Answer:**

> * **All-Purpose Clusters** are used for interactive development and exploratory analysis.
> * **Job Clusters** are transient clusters automatically created and terminated for scheduled or production jobs.
> * **SQL Warehouses** are optimized clusters designed for executing SQL queries and powering BI dashboards.
