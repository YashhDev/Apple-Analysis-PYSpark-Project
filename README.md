# 📊 Databricks ETL Pipeline with Factory Pattern & PySpark

![Python](https://img.shields.io/badge/Python-3.10-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)
![Databricks](https://img.shields.io/badge/Databricks-Cloud-red.svg)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-brightgreen.svg)
![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)

---

## 📌 Project Overview

This project demonstrates how to design **scalable ETL pipelines** in **Databricks** using **PySpark** and the **Factory Pattern** for multiple data sources.  

We ingest data from:
- **CSV files**
- **Parquet files**
- **Delta Tables**

The pipeline is modular, with a **Reader → Transformer → Loader** architecture, allowing easy extension to additional data sources.

---

## 🏗 Architecture


flowchart LR
    A[CSV / Parquet / Delta Table] -->|Reader Factory| B[PySpark DataFrame]
    B --> C[Transformation Logic (PySpark SQL)]
    C -->|Data Lake| D[ADLS Bronze/Silver/Gold]
    C -->|Data Lakehouse| E[Delta Tables]
🛠 Tech Stack
Databricks (Development & Orchestration)

Apache Spark with PySpark

Factory Pattern (Low-level design for multi-source ingestion)

Azure Data Lake Storage (ADLS) – Data Lake

Delta Lake – Data Lakehouse

Spark SQL & PySpark DataFrame API for transformations


databricks-etl-factory/
│
├── src/
│   ├── reader_factory.py     # Implements Factory Pattern for multiple sources
│   ├── transformations.py    # Business transformation logic
│   ├── loader.py              # Writes to Data Lake & Lakehouse
│
├── data/
│   ├── csv/                   # Sample CSV files
│   ├── parquet/               # Sample Parquet files
│   ├── delta/                 # Sample Delta tables
│
├── notebooks/
│   ├── etl_pipeline.ipynb     # Main Databricks ETL notebook
│
└── README.md


Business Logic Implemented
Customers who bought AirPods after iPhone

SELECT customer_id
FROM purchases
WHERE product = 'Airpods'
  AND purchase_date > (
      SELECT MIN(purchase_date)
      FROM purchases
      WHERE product = 'iPhone'
        AND purchases.customer_id = customer_id
  )



Customers who bought both AirPods and iPhone

SELECT customer_id
FROM purchases
GROUP BY customer_id
HAVING COUNT(DISTINCT product) FILTER (WHERE product IN ('Airpods', 'iPhone')) = 2



List all products bought by customers after their initial purchase

SELECT customer_id, product, purchase_date
FROM (
    SELECT customer_id, product, purchase_date,
           MIN(purchase_date) OVER (PARTITION BY customer_id) AS first_purchase
    FROM purchases
)
WHERE purchase_date > first_purchase

📸 Sample Output
customer_id	product	purchase_date
101	Airpods	2024-06-12
102	iPhone	2024-05-21
103	MacBook	2024-07-05

📈 Key Highlights
Factory Pattern for easily extending to more data sources.

Modular pipeline with clear separation between Reader, Transformer, and Loader.

Supports both Data Lake & Data Lakehouse destinations.

Fully scalable for large datasets in Databricks.

