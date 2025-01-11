import pandas as pd
from pymongo import MongoClient
import jdatetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import uuid

# Function to load CSV file
def load_csv(file_path):
    print("Loading CSV file...")
    return pd.read_csv(file_path, low_memory=False)

# Function to clean data
def clean_data(df):
    print("Cleaning data...")

    # Remove 'Unnamed: 0' column if it exists
    if 'Unnamed: 0' in df.columns:
        print("Removing 'Unnamed: 0' column...")
        df.drop(columns=['Unnamed: 0'], inplace=True)

    # Add unique identifiers
    print("Adding unique identifiers (product_id, sales_id)...")
    df['product_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['sales_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Clean text from 'actual_price' column
    if 'actual_price' in df.columns:
        print("Cleaning 'actual_price' column...")
        df['actual_price'] = df['actual_price'].replace('[^\d.]', '', regex=True)
        df['actual_price'] = pd.to_numeric(df['actual_price'], errors='coerce')  # Convert to float, set invalid values to NaN
    else:
        print("Column 'actual_price' not found, adding it with default None values...")
        df['actual_price'] = None  # If column doesn't exist, add it as None

    # Drop rows where 'actual_price' is NaN
    print("Dropping rows with NaN in 'actual_price'...")
    df = df.dropna(subset=['actual_price'])

    # Ensure 'actual_price' has float type
    print("Converting 'actual_price' to float...")
    df['actual_price'] = df['actual_price'].astype(float).round(1)

    # Add a new column 'sales_price' with calculated values
    print("Calculating 'sales_price' column...")
    df['sales_price'] = df['actual_price'] * 0.9  # Example: 10% discount from 'actual_price'

    # Convert 'date' column to string format and handle invalid data
    if 'date' in df.columns:
        print("Converting 'date' column to Jalali format...")
        df['date'] = df['date'].astype(str)
        df['date'] = df['date'].apply(convert_to_jalali)

    # Drop rows with invalid dates
    print("Dropping rows with invalid 'date' values...")
    df = df.dropna(subset=['date'])

    return df

# Function to convert Gregorian date to Jalali
def convert_to_jalali(date):
    try:
        gregorian_date = pd.to_datetime(date, errors='coerce')
        if pd.isnull(gregorian_date):
            return None
        jalali_date = jdatetime.date.fromgregorian(
            year=gregorian_date.year,
            month=gregorian_date.month,
            day=gregorian_date.day
        )
        return f"{jalali_date.year:04d}-{jalali_date.month:02d}-{jalali_date.day:02d}"
    except Exception:
        return None

# Function to split DataFrame into separate DataFrames
def split_dataframes(df):
    print("Splitting data into separate DataFrames...")
    product_price_df = df[['product_id', 'ratings', 'no_of_ratings', 'discount_price', 'actual_price']].copy()
    product_details_df = df[['product_id', 'name', 'main_category', 'sub_category', 'image', 'link']].copy()
    sales_df = df[['sales_id', 'product_id', 'date', 'sales_price']].copy()

    # Display DataFrames
    print("\n--- Product Price DataFrame ---")
    print(product_price_df.head())
    print("\n--- Product Details DataFrame ---")
    print(product_details_df.head())
    print("\n--- Sales DataFrame ---")
    print(sales_df.head())

    return product_price_df, product_details_df, sales_df

# Function to save data to MongoDB
def save_to_mongodb(df, collection_name):
    print(f"Saving DataFrame to MongoDB collection '{collection_name}'...")
    client = MongoClient('mongodb://192.168.8.133:27017/')
    db = client.amazon_db
    collection = db[collection_name]
    collection.insert_many(df.to_dict('records'))
    print(f"Data saved to '{collection_name}' successfully.")

# ETL process function
def etl_process():
    print("Starting ETL process...")

    # Load CSV file
    file_path = "/home/mongod/amazon-products.csv"
    df = load_csv(file_path)

    # Clean data
    df = clean_data(df)

    # Split data into separate DataFrames
    product_price_df, product_details_df, sales_df = split_dataframes(df)

    # Save each DataFrame to MongoDB
    save_to_mongodb(product_price_df, "product_price_collection")
    save_to_mongodb(product_details_df, "product_details_collection")
    save_to_mongodb(sales_df, "sales_collection")

    print("ETL process completed successfully.")

# Define the Airflow DAG
define_dag = DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
)

# Define the ETL task
etl_task = PythonOperator(
    task_id="run_etl_process",
    python_callable=etl_process,
    dag=define_dag
)

if __name__ == "__main__":
    etl_process()

