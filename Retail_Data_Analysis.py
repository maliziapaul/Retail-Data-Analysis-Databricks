# Databricks notebook source
# MAGIC %md
# MAGIC # Question 2: Retail Data Analysis
# MAGIC
# MAGIC Retail sales from 305 daily CSV files (`retail-data/by-day`), analysed with PySpark in Databricks.
# MAGIC
# MAGIC **Product Value = Unit Price × Quantity**
# MAGIC
# MAGIC | Part | Task |
# MAGIC |---|---|
# MAGIC | a | Upload the data files to the Databricks file system |
# MAGIC | b | Total number of transactions and total product value |
# MAGIC | c | Top 5 best-selling products by quantity |
# MAGIC | d | Top 5 most valuable products by sales value |
# MAGIC | e | Total purchase value by country |
# MAGIC | f | Visualization of part d |
# MAGIC | g | Export as a `.py` file |

# COMMAND ----------

# MAGIC %md
# MAGIC ## a) Upload the data files to the Databricks file system
# MAGIC
# MAGIC The 305 CSV files were uploaded through **Catalog → workspace → default → Create → Volume** (`retail_data`), then **Upload to this volume**.
# MAGIC Unity Catalog volumes are the Databricks file system for files in current workspaces. The cell below lists what was uploaded.

# COMMAND ----------

from pyspark.sql import functions as F

DATA_DIR = "/Volumes/workspace/default/retail_data"   # change if you uploaded somewhere else

files = dbutils.fs.ls(DATA_DIR)
csv_files = [f for f in files if f.name.endswith(".csv")]
print(f"{len(csv_files)} CSV files, {sum(f.size for f in csv_files) / 1e6:.1f} MB in {DATA_DIR}")
display(spark.createDataFrame([(f.name, f.size) for f in csv_files], "FileName string, SizeBytes long").orderBy("FileName"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## b) Total transactions and total product value
# MAGIC
# MAGIC Each row in the files is one transaction (one product line on an invoice).
# MAGIC The distinct invoice count is shown too, since several rows can share one invoice.
# MAGIC Returns and cancellations (negative quantities) are included, so the total value is net of them.

# COMMAND ----------

retail = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(DATA_DIR)
          .withColumn("ProductValue", F.col("UnitPrice") * F.col("Quantity")))

retail.printSchema()

totals = retail.agg(
    F.count("*").alias("TotalTransactions"),
    F.countDistinct("InvoiceNo").alias("DistinctInvoices"),
    F.round(F.sum("ProductValue"), 2).alias("TotalProductValue"),
)
display(totals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## c) Top 5 best-selling products by total quantity sold

# COMMAND ----------

top5_quantity = (retail
                 .where(F.col("Description").isNotNull())
                 .groupBy("Description")
                 .agg(F.sum("Quantity").alias("TotalQuantity"))
                 .orderBy(F.desc("TotalQuantity"))
                 .limit(5))
display(top5_quantity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## d) Top 5 most valuable products by total sales value
# MAGIC
# MAGIC "DOTCOM POSTAGE" is a shipping charge recorded as a line item. It is kept because the question covers all transactions.

# COMMAND ----------

top5_value = (retail
              .where(F.col("Description").isNotNull())
              .groupBy("Description")
              .agg(F.round(F.sum("ProductValue"), 2).alias("TotalSalesValue"))
              .orderBy(F.desc("TotalSalesValue"))
              .limit(5))
display(top5_value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## e) Total value of purchases by country

# COMMAND ----------

country_totals = (retail
                  .groupBy("Country")
                  .agg(F.round(F.sum("ProductValue"), 2).alias("TotalPurchaseValue"))
                  .orderBy(F.desc("TotalPurchaseValue")))
print(f"{country_totals.count()} countries")
display(country_totals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## f) Visualization of the top 5 most valuable products (part d)
# MAGIC
# MAGIC Bar chart added with **+ → Visualization** on this cell's result: X axis `Description`, Y axis `TotalSalesValue`.

# COMMAND ----------

display(top5_value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## g) Export
# MAGIC
# MAGIC Exported with **File → Export → Source file**, which produces `Retail_Data_Analysis.py`.
