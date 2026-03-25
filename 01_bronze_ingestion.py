# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer - Raw Data Ingestion
# MAGIC **Purpose:** Ingest raw CSV data into Delta Lake without any transformation.
# MAGIC Raw data is preserved as-is for full traceability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure File Path

# COMMAND ----------

# Define the path to the raw CSV file uploaded to DBFS
raw_data_path = "/FileStore/ecommerce/sample_ecommerce_data.csv"
bronze_path    = "/delta/ecommerce/bronze"

print(f"Raw Data Path : {raw_data_path}")
print(f"Bronze Path   : {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read Raw CSV Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Read raw CSV with header and inferred schema
df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_data_path)
)

print(f"Total Records Ingested: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Ingestion Metadata

# COMMAND ----------

# Add ingestion timestamp for auditability
df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Delta Lake (Bronze)

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .save(bronze_path)
)

print("✅ Bronze layer written successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Register as Temp View

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_ecommerce")
print("✅ Temp view 'bronze_ecommerce' created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Quick Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows FROM bronze_ecommerce;
