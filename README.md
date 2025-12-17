# Lab 2 — End-to-End Stock Data Analytics Pipeline  
(Airflow → Snowflake → dbt → Superset)**

## **📘 Overview**

This project implements a complete, production-style **ELT data pipeline** used to automate end-to-end stock analytics.  
Daily market data is extracted using **yfinance**, loaded into **Snowflake**, transformed with **dbt**, and visualized in **Apache Superset**.

The project demonstrates the core concepts of enterprise data engineering:

- Workflow orchestration with **Airflow**
- Cloud data warehousing using **Snowflake**
- Analytics engineering and modeling with **dbt**
- Dashboarding and BI using **Superset**

---

## **🔄 Architecture**

**Extract → Load → Transform → Visualize**

