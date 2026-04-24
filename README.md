# E-CommerceETLProject

# 🛒 E-Commerce ETL Pipeline

This project demonstrates a complete **end-to-end ETL (Extract, Transform, Load)** pipeline for processing e-commerce data using distributed technologies such as **Apache Spark**, **Hive**, **Amazon S3**, and **ClickHouse**. The solution ingests data from **AWS RDS** and **MongoDB**, transforms it using **Spark**, stores it using **Hive & S3**, and analyzes it using **ClickHouse**.

---

## 📌 Project Overview

The goal is to create a scalable and modular data pipeline that processes both structured and semi-structured transactional data, performs transformation and validation, applies Slowly Changing Dimension (SCD) tracking, and finally loads the refined data for downstream analytics.

---

## 🧩 Tech Stack

## 🛠️ Tools & Versions

+-------------+------------+
| Tool        | Version    |
+-------------+------------+
| Amazon S3   | N/A        |
| AWS RDS     | 17.2-R2    |
| Python      | 3.9.2      |
| Pandas      | 2.3.1      |
| Boto3       | 1.40.0     |
| EMR         | 7.8.0      |
| PuTTY       | 0.83       |
| Hadoop      | 3.4.1      |
| Hive        | 3.1.3      |
| Spark       | 3.5.4      |
| MongoDB     | 6.0.4      |
| ClickHouse  | 25.6.5.41  |
+-------------+------------+

---

## 🧠 Roles & Responsibilities

### 1. 🔄 Data Extraction & Ingestion
- Extract raw transactional data from **AWS RDS** into **Apache Spark** (via JDBC on EMR).
- Ingest **JSON data** from **MongoDB** into **Amazon S3 (L1)** using **Pandas + PyMongo**.

### 2. 🧪 Data Transformation & Processing
- Merge both data sources (RDS + MongoDB data).
- Apply transformation logic in Spark:
  - Filtering
  - Joins
  - Aggregations
  - Derived columns

### 3. ✅ Data Validation
- Conduct the following checks:
  - Consistency check
  - NULL check
  - Duplicate check
  - Schema validation
- Track changes such as:
  - **Order cancellations**
  - **Phone number changes**
  - **Email address changes**
  - **Address updates**
  - Using **SCD-1** and **SCD-4**

### 4. 📥 Data Loading
- Load transformed data into **Hive**.
- Write clean data into **Amazon S3 (L2)** to ensure durability.

### 5. 📊 Business KPIs Calculation
Calculated KPIs include:
- Average Order Value
- Repeat Customer Count
- Order Cancellation Rate
- Daily Order Processing
- Order Success Rate
- Top Customer by Revenue

### 6. 🔍 Querying & Analysis
- Load the final dataset from **S3 L2** into **ClickHouse**.
- Run analytical queries to support business insights.

---

## 🗂️ Project Structure

E-Commerce-ETL/
│
├── 1) RDS.txt # RDS Configuration and connection details
├── 2) RDTOSPARK.py # Extract data from AWS RDS into Spark via JDBC
├── 3) MONGOtoS3L1.py # Extract MongoDB JSON data and upload to S3 (L1)
├── 4) S3L1toSPARK.py # Load S3 L1 data into Spark for processing
├── 5) VALIDATION.py # Perform data validation checks (NULLs, Duplicates, etc.)
├── 6) BUSINESSKPI.py # Calculate key business metrics like AOV, cancellations, etc.
├── 7) SPARKtoHIVE.py # Store transformed data from Spark into Hive tables
├── 8) HIVEtoS3L2.py # Export data from Hive to S3 (L2) for durability
├── 9) S3L1toCLICKHOUSE.py # Load S3 L2 data into ClickHouse for querying
├── ProjectArchitecture.png # Visual representation of the ETL pipeline
└── README.md # Project documentation (this file)
