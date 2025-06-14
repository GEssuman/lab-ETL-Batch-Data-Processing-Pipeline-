
# Batch Data Processing Pipeline for Rental Marketplace Analytics

## Project Overview

This project implements a scalable **batch data processing pipeline** designed to support **analytics** and **business intelligence** for a **rental marketplace platform** (similar to Airbnb). The operational data originates from an **AWS Aurora MySQL database**, and the pipeline transforms and loads this data into **Amazon Redshift**, the central data warehouse, for downstream reporting and analysis.

> **Note:** This project is still **in progress**.

---

## Architecture

This solution follows the **Medallion Architecture (Bronze → Silver → Gold)** pattern using **Amazon S3** as the storage layer:

- `raw_data/` → Bronze: Raw ingested data  
- `curated/` → Silver: Cleaned and transformed data  
- `presentation/` → Gold: Aggregated insights ready for analytics  

The pipeline processes data in batches, ensuring accuracy, traceability, and scalability for large datasets.

---

## Deployment Options

### Cloud Deployment (AWS)
In a full cloud setup:
- **Data Source:** AWS Aurora MySQL  
- **Processing:** AWS Glue & PySpark  
- **Storage:** Amazon S3 (Medallion zones)  
- **Data Warehouse:** Amazon Redshift  
- **Orchestration:** AWS Step Functions (planned)  

> Orchestration using AWS Step Functions is **planned** for production-grade pipeline scheduling and monitoring.

---

### Local Development Setup
For development and testing purposes, the pipeline can also run **locally** with the following substitutions:
- **PostgreSQL** is used instead of Redshift.
- Each Medallion stage (raw, curated, presentation) maps to a **different PostgreSQL schema**.

This setup provides fast iteration and validation before deploying to the cloud.

---

## Tech Stack

| Layer          | Technology        |
|----------------|-------------------|
| Ingestion      | AWS Glue / PySpark|
| Storage        | Amazon S3         |
| Transformation | PySpark           |
| Warehouse      | Amazon Redshift / PostgreSQL |
| Orchestration  | AWS Step Functions (planned) |

---

##  TODO

- [ ] Integrate AWS Step Functions for orchestration  
- [ ] Implement Redshift table schema creation via Glue  
- [ ] Add data validation and quality checks  
- [ ] Add System Architectur Design  

---


