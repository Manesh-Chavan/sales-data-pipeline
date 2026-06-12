
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

# ── Task 1: Ingest ──
def ingest_data():
    DATA_URL = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    RAW_PATH = "/home/jovyan/sales-data-pipeline/data/raw/sales_raw.csv"
    os.makedirs("/home/jovyan/sales-data-pipeline/data/raw", exist_ok=True)
    response = requests.get(DATA_URL)
    with open(RAW_PATH, "wb") as f:
        f.write(response.content)
    print("✅ Ingestion complete!")

# ── Task 2: Transform ──
def transform_data():
    spark = SparkSession.builder.appName("SalesPipeline").getOrCreate()
    df = spark.read.csv(
        "/home/jovyan/sales-data-pipeline/data/raw/sales_raw.csv",
        header=True, inferSchema=True)

    df_clean = df         .withColumnRenamed('Index', 'id')         .withColumnRenamed(' Height(Inches)"', 'height_inches')         .withColumnRenamed(' "Weight(Pounds)"', 'weight_pounds')

    df_transformed = df_clean         .withColumn("height_cm", round(col("height_inches") * 2.54, 2))         .withColumn("weight_kg", round(col("weight_pounds") * 0.453592, 2))         .withColumn("bmi", round(
            col("weight_kg") / ((col("height_cm") / 100) ** 2), 2))         .withColumn("bmi_category",
            when(col("bmi") < 18.5, "Underweight")
            .when(col("bmi") < 25.0, "Normal")
            .otherwise("Overweight"))

    df_transformed.write.csv(
        "/home/jovyan/sales-data-pipeline/data/processed/",
        header=True, mode="overwrite")
    print("✅ Transformation complete!")
    spark.stop()

# ── Task 3: Save ──
def save_data():
    import glob
    files = glob.glob("/home/jovyan/sales-data-pipeline/data/processed/part-*.csv")
    df = pd.read_csv(files[0])
    df.to_csv(
        "/home/jovyan/sales-data-pipeline/data/processed/final_output.csv",
        index=False)
    print(f"✅ Saved {len(df)} rows to final_output.csv!")

# ── DAG Definition ──
default_args = {
    'owner': 'manesh',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sales_data_pipeline',
    default_args=default_args,
    description='End-to-end sales data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    task_save = PythonOperator(
        task_id='save_data',
        python_callable=save_data
    )

    # Pipeline order
    task_ingest >> task_transform >> task_save
