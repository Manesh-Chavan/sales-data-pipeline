\# End-to-End Sales Data Pipeline



\## Overview

A fully functional data engineering pipeline built to mirror Azure cloud architecture using free, open-source tools.



\## Architecture

Source → Ingestion → Raw Storage → PySpark Transform → SQL Server



\## Azure Equivalent Stack

| Local Tool | Azure Equivalent |

|------------|-----------------|

| Python + requests | Azure Data Factory |

| Local folder | Azure Data Lake Storage |

| PySpark | Azure Databricks |

| SQL Server | Azure SQL Database |



\## Pipeline Steps

1\. Ingest raw CSV data from public source

2\. Store raw data locally (Data Lake simulation)

3\. Transform using PySpark — added height\_cm, weight\_kg, BMI, BMI category

4\. Load final data into SQL Server

5\. Run analytical queries on processed data



\## Key Results

\- Processed 200 records

\- 150 Normal BMI (avg 20.02)

\- 50 Underweight (avg 17.4)



\## How to Run

1\. Run `ingestion/ingest.py` to download raw data

2\. Run `transformation/transform.py` for PySpark transforms

3\. Import `final\_output.csv` into SQL Server

