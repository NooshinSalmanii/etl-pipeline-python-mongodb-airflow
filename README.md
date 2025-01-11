# ETL Pipeline: Python, MongoDB, and Airflow

This project automates an ETL process to extract data from a CSV file, clean and process it, and load it into MongoDB. The pipeline is managed and monitored using Apache Airflow.

---

## Features

- **ETL Workflow**: Automated data extraction, cleaning, and loading.
- **MongoDB Integration**: Data stored in structured collections.
- **Apache Airflow**: Used for pipeline orchestration.
- **Environment Isolation**: Virtual environment created using Conda.

---

## Requirements

- **Operating System**: CentOS Stream 8
- **Python**: 3.9 or above
- **MongoDB**: Local or remote instance
- **Apache Airflow**: Installed and configured

---

## Quick Start

1. **Set up Conda environment**:
   conda create --name air_env python=3.9 -y
   conda activate air_env
   conda install pandas
   conda install pymongo openpyxl
   conda install -c conda-forge apache-airflow
   pip3 install jdatetime


2. Run MongoDB:
Ensure MongoDB is running locally or configure the remote connection.

3. Run airflow:
airflow db init
airflow webserver
airflow scheduler

4. Execute the pipeline:
Directly run the script:
python3 etl_pipeline.py



Pipeline Workflow
Step-by-Step Process:
1. Extract: 
- Load the data from a CSV file.
2. Transform:
- Remove unnecessary columns (e.g., Unnamed: 0).
- Add unique IDs for products and sales.
- Calculate the sales_price as 90% of the actual_price.
- Convert actual_price to float and clean text data.
- Convert Gregorian dates to Jalali format.
3. Load: 
Store the processed data into three MongoDB collections:
- product_price_collection
- product_details_collection
- sales_collection
