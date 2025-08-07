
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_ecommerce_pipeline',
    default_args=default_args,
    description='Simple ecommerce data pipeline',
    schedule_interval='@daily',
    catchup=False
)

def load_csv_data():
    """Load CSV data to database"""
    print("Loading CSV data...")
    
    # Read CSV with correct encoding
    df = pd.read_csv('/opt/airflow/data/ecommerce_data.csv', encoding='latin-1')
    print(f"Read {len(df)} records")
    print(f"Columns: {list(df.columns)}")
    
    # Clean column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Create SQLAlchemy engine (proper way for PostgreSQL)
    engine = create_engine('postgresql://admin:password123@postgres:5432/ecommerce')
    
    # Load to database
    df.to_sql('ecommerce_data', engine, if_exists='replace', index=False)
    
    print(f"Successfully loaded {len(df)} records to database")

# Create task
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_csv_data,
    dag=dag
)
