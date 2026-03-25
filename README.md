# E-Commerce-Data-Pipeline

E-Commerce Data Pipeline | Databricks
# An end-to-end data pipeline built on Databricks Free Edition using Medallion Architecture (Bronze → Silver → Gold). Raw e-commerce data is ingested, cleaned, and transformed using PySpark and Spark SQL, with Delta Lake handling storage and Unity Catalog managing governance.
Tech Stack
# PySpark, Apache Spark, Delta Lake, Spark SQL, Databricks, Unity Catalog
Architecture
# Raw data flows through three layers — Bronze (raw ingestion), Silver (cleaned and typed), and Gold (business aggregations) — following the Medallion Architecture pattern.

Notebooks

# 01_bronze_ingestion — Loads raw CSV into Delta Lake
# 02_silver_transformation — Cleans, deduplicates, and casts data types
# 03_gold_aggregation — Builds revenue summaries by category, country, and customer

Key Highlights

# ACID-compliant storage with Delta Lake
# Data governance using Unity Catalog
# Query optimization via execution plans and partitioning
