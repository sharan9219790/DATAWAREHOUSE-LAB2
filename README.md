# **Lab 2 — End-to-End Stock Data Analytics Pipeline  
(Airflow → Snowflake → dbt → Superset)**

## **📘 Overview**

This project implements a complete, production-oriented **ELT (Extract-Load-Transform) data pipeline** that automates daily stock analytics using modern data engineering tools.  

The pipeline performs:

1. **Extraction** — Download stock price data from Yahoo Finance (`yfinance`)
2. **Loading** — Store raw data in **Snowflake**
3. **Transformation** — Clean & enrich data using **dbt**
4. **Visualization** — Build insights dashboards in **Apache Superset**

This lab demonstrates real-world orchestration, warehousing, transformation modeling, and BI integration.

---

## **🔄 Architecture Diagram**

