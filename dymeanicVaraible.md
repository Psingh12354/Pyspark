# 🚀 Passing Dynamic Variables in Databricks

Dynamic variables make your **Databricks notebooks, jobs, and pipelines** reusable, scalable, and environment-independent.
You can dynamically control **paths, environments, parameters, and credentials** using various techniques.

---

## 🧭 1. Using Databricks Widgets

Widgets are UI elements that let users **input parameters dynamically** at runtime.

### 🧩 Example

```python
# Create a text widget
dbutils.widgets.text("input_path", "/mnt/raw/data.csv", "Input File Path")

# Retrieve the widget value
input_path = dbutils.widgets.get("input_path")

# Use it in your code
df = spark.read.option("header", True).csv(input_path)
display(df)
```

### 🖥️ Sample Output

```
Input File Path: /mnt/raw/data.csv
Data Preview:
+-------+-----+
| Name  | Age |
+-------+-----+
| John  |  30 |
| Alice |  25 |
+-------+-----+
```

💡 **Tip:** You can also use dropdowns, combo boxes, and multiselect widgets.

```python
dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Select Environment")
env = dbutils.widgets.get("env")
print(f"Environment selected: {env}")
```

---

## 🔗 2. Passing Variables Between Notebooks

You can pass parameters **from one notebook to another** using `dbutils.notebook.run()`.

### 📘 Parent Notebook

```python
result = dbutils.notebook.run(
    "/Shared/child_notebook",
    timeout_seconds=60,
    arguments={"input_path": "/mnt/raw/customers.csv", "env": "dev"}
)
print(result)
```

### 📗 Child Notebook

```python
input_path = dbutils.widgets.get("input_path")
env = dbutils.widgets.get("env")

print(f"Reading from {input_path} in {env} environment.")
```

### 🖥️ Sample Output

```
Reading from /mnt/raw/customers.csv in dev environment.
```

---

## ⚙️ 3. Databricks Job Parameters

When running notebooks as **Databricks Jobs**, you can define parameters directly in the job UI or JSON config.

### Example

* **Job Parameter Setup**

  ```
  input_path = /mnt/prod/data.csv
  env = prod
  ```

### Notebook Code

```python
input_path = dbutils.widgets.get("input_path")
env = dbutils.widgets.get("env")
print(f"Processing {input_path} in {env} mode.")
```

### 🖥️ Sample Output

```
Processing /mnt/prod/data.csv in prod mode.
```

💡 Best for **production workflows** where jobs run automatically.

---

## 🌍 4. Using Environment Variables

You can store and access variables as **environment-level parameters**.

### Example

```python
import os

# Set environment variables
os.environ["env"] = "qa"
os.environ["input_path"] = "/mnt/qa/data/"

# Access environment variables
env = os.getenv("env")
input_path = os.getenv("input_path")

print(f"Environment: {env}, Input Path: {input_path}")
```

### 🖥️ Sample Output

```
Environment: qa, Input Path: /mnt/qa/data/
```

💡 Works great for **CI/CD pipelines** or **cluster-scoped settings**.

---

## 🗂️ 5. Using Configuration Files (JSON/YAML)

You can externalize parameters into a **config file** for flexible multi-environment setups.

### config.json

```json
{
  "dev": {
    "input_path": "/mnt/dev/data/",
    "output_path": "/mnt/dev/output/"
  },
  "prod": {
    "input_path": "/mnt/prod/data/",
    "output_path": "/mnt/prod/output/"
  }
}
```

### Notebook Code

```python
import json

env = "dev"
config = json.load(open("/dbfs/FileStore/config.json"))

input_path = config[env]["input_path"]
output_path = config[env]["output_path"]

print(f"Input: {input_path}")
print(f"Output: {output_path}")
```

### 🖥️ Sample Output

```
Input: /mnt/dev/data/
Output: /mnt/dev/output/
```

💡 Ideal for **multi-environment configuration management**.

---

## 🔒 6. Using Databricks Secrets

Use **Databricks Secrets** to securely store sensitive data like credentials or API keys.

### Example

```python
db_password = dbutils.secrets.get(scope="myscope", key="db-password")
print("Secret retrieved securely!")
```

### 🖥️ Sample Output

```
Secret retrieved securely!
```

💡 Secrets are **hidden from logs and UI**, ensuring **data security**.

---

## 🔁 7. Using ADF / Airflow / REST API Parameters

If Databricks is triggered from an external orchestrator like **Azure Data Factory** or **Airflow**,
you can pass parameters dynamically at runtime.

### Example (ADF)

```json
"baseParameters": {
  "input_path": "@pipeline().parameters.input_path",
  "process_date": "@utcNow()"
}
```

### Notebook Code

```python
input_path = dbutils.widgets.get("input_path")
process_date = dbutils.widgets.get("process_date")

print(f"Processing {input_path} for date {process_date}")
```

### 🖥️ Sample Output

```
Processing /mnt/raw/data/ for date 2025-10-16
```

---

## 📊 Summary Table

| Method                       | Description                              | Best Use Case                |
| ---------------------------- | ---------------------------------------- | ---------------------------- |
| **Widgets**                  | UI input elements for runtime parameters | Manual runs, ad-hoc analysis |
| **Notebook Chaining**        | Pass parameters between notebooks        | Modular workflows            |
| **Job Parameters**           | Define parameters in Databricks Jobs     | Production pipelines         |
| **Environment Variables**    | Use OS/cluster-level variables           | CI/CD integration            |
| **Config Files (JSON/YAML)** | Centralized configuration management     | Multi-environment setups     |
| **Secrets**                  | Secure sensitive values                  | Credentials, tokens          |
| **ADF / Airflow**            | External orchestration parameter passing | End-to-end ETL automation    |

---

## 🧠 Example Real-World Use Case

Imagine a **daily ETL pipeline** that processes data by date and environment:

```python
# Widgets
dbutils.widgets.text("process_date", "2025-10-16", "Date")
dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")

# Get values
process_date = dbutils.widgets.get("process_date")
env = dbutils.widgets.get("env")

# Use config
import json
config = json.load(open("/dbfs/FileStore/config.json"))
input_path = config[env]["input_path"]

print(f"Running ETL for {process_date} in {env} environment using {input_path}")
```

### 🖥️ Sample Output

```
Running ETL for 2025-10-16 in dev environment using /mnt/dev/data/
```

---

## 🏁 Conclusion

By using these dynamic variable-passing techniques, you can:

* Make your Databricks notebooks **reusable**
* Integrate them with **orchestration tools**
* Manage **multi-environment configurations**
* Ensure **security** for sensitive information

---

### 🔗 References

* [Databricks Widgets Documentation](https://docs.databricks.com/en/notebooks/widgets.html)
* [Databricks Secrets Management](https://docs.databricks.com/en/security/secrets/secrets.html)
* [Databricks Jobs Parameters](https://docs.databricks.com/en/jobs/index.html)

---
