# azure-data-engineer---multi-source

## Project Overview

This project implements an **end-to-end retail data pipeline** using Azure Data Engineering tools. The pipeline ingests data from multiple sources, transforms it into a structured data lake format (Bronze → Silver → Gold layers), and enables business reporting through Power BI.

---

## Tech Stack:

- Azure Data Factory (ADF) → Data ingestion

- Azure Data Lake Storage (ADLS) → Central data lake (Bronze/Silver/Gold)

- Azure Databricks → Data cleaning, transformation, aggregation

- Power BI → Reporting & dashboards

---

## 📂 Data Sources

**1.** **Azure SQL Database**

-**Transactions**(sales data)

- **Stores** (store details)

- **Products** (catalog details)

**2.** API / JSON

- Customers (customer master data in JSON format)

---

## 🏗️ Architecture
```
graph LR

A[Azure SQL - Transactions] --> B[ADF]
C[Azure SQL - Stores] --> B
D[Azure SQL - Products] --> B
E[API - Customers JSON] --> B
B --> F[ADLS - Bronze]
F --> G[Databricks - Silver Layer]
G --> H[Databricks - Gold Layer]
H --> I[Power BI Reports]
```
---

- **Bronze Layer** → Raw ingestion from SQL & JSON

- **Silver Layer** → Cleaned, standardized, joined dataset

- **Gold Layer** → Aggregated business-level KPIs for reporting.

---

## ⚙️ Data Processing
**Bronze Layer**

- Raw parquet files from ADF ingestion

- Stores, Products, Transactions, Customers as-is

**Silver Layer**

- Cleaned using PySpark (```retail projects - multiple tables (1).py```):

- Schema alignment (casting datatypes)

- Removing duplicates

- Joining customers, stores, products, transactions

- Adding derived column: ```total_amount = quantity * price```

**Gold Layer**

- Aggregated metrics created:

- ```total_quantity_sold```

- ```total_sales_amount```

- ```number_of_transactions```

- ```average_transaction_value```

Stored as Delta tables for optimized analytics.

## 📊 Business Requirements & KPIs
**1.** Total Sales by Store and Category

- Metric: SUM(total_sales_amount)

- Dimensions: Store, Category

- Visualization: Grouped Bar Chart

**2.** Daily Sales Trend by Product

- Metric: SUM(total_sales_amount)

- Dimensions: Date, Product

- Visualization: Line Chart (time series)

**3.** Average Order Value per Store

- Metric: ```SUM(total_sales_amount) / COUNT(transaction_id)```

- Visualization: Column Chart (per store)

**4.** Heatmap: Store vs Sales

- **Metric:** SUM(total_sales_amount)

- **Dimensions:** Store × Product/Category

- **Visualization:** Heatmap (intensity by sales)

---

## 📑 Power BI Reporting

- Connect Power BI to Gold Layer Delta tables

- Create dashboard with:

   - 📊 Sales by Store & Category

  - 📈 Daily Sales Trend by Product

   - 💰 AOV per Store

   - 🔥 Heatmap: Store vs Sales
 
---

## 🚀 Deployment Steps

**1.**  Deploy SQL schema & sample data (SCRIPT SQL.txt)

**2.**  Upload customers.json to API/Blob for ingestion

**3.**  Configure ADF pipelines:

- Copy data from SQL & JSON to ADLS Bronze

**4.**  Run Databricks notebook (retail projects - multiple tables (1).py)

- Create Silver & Gold tables

**5.**  Connect Power BI → Gold Layer Delta tables

**6.**  Build dashboards as per KPIs

```
📦 Repository Structure
├── customers.json                  # Customer data (JSON)
├── SCRIPT SQL.txt                  # SQL schema + inserts
├── retail projects - multiple tables (1).py   # Databricks ETL script
├── docs/
│   ├── architecture.png            # Architecture diagram
│   └── dashboard_wireframes.png    # Power BI mockups
└── README.md                       # Project documentation
```
---
## 📌 Future Enhancements

- Automate pipeline scheduling with ADF triggers

- Add incremental data loads (CDC for transactions)

- Implement role-based security in Power BI

- Integrate ML models for demand forecasting

---
