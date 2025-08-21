from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from sqlalchemy import create_engine
import time
from monitoring import log_pipeline_start, log_pipeline_success, log_pipeline_failure

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'complete_elt_pipeline',
    default_args=default_args,
    description='Complete ELT pipeline with dbt: Extract, Load, Transform',
    schedule_interval='@daily',
    catchup=False
)

def extract_and_load():
    """ELT Step 1&2: Extract and Load raw data"""
    start_time = time.time()
    log_pipeline_start()  # Log start to PostgreSQL
    
    try:
        print("🔄 ELT Phase: Extract and Load")
        
        # Read CSV with correct encoding
        df = pd.read_csv('/opt/airflow/data/ecommerce_data.csv', encoding='latin-1')
        print(f"✅ Extracted {len(df)} records from CSV")
        
        # Clean column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Data Quality: Drop rows with missing critical data
        critical_columns = ['invoiceno', 'stockcode', 'unitprice', 'quantity']
        initial_count = len(df)
        df = df.dropna(subset=critical_columns)
        print(f"🧹 Dropped {initial_count - len(df)} rows with missing critical data")
        
        # Data Quality: Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        print(f"🧹 Removed {initial_count - len(df)} duplicate rows")
        
        print(f"📊 Final clean dataset: {len(df)} records")
        
        # Load raw data to database
        engine = create_engine('postgresql://admin:password123@postgres:5432/ecommerce')
        df.to_sql('ecommerce_data', engine, if_exists='replace', index=False)
        
        # Calculate duration and log success
        duration = time.time() - start_time
        log_pipeline_success(len(df), duration)
        
        print(f"✅ Loaded {len(df)} clean records to database")
        print(f"📊 Pipeline completed in {duration:.2f} seconds")
        
    except Exception as e:
        log_pipeline_failure(str(e))  # Log failure to PostgreSQL
        print(f"❌ Pipeline failed: {e}")
        raise

# Define tasks
extract_load = PythonOperator(
    task_id='extract_and_load_raw_data',
    python_callable=extract_and_load,
    dag=dag
)

# Create dbt project and run transformations
setup_and_run_dbt = BashOperator(
    task_id='setup_and_run_dbt',
    bash_command='''
    # Create dbt project directory
    mkdir -p /tmp/dbt_project
    cd /tmp/dbt_project
    
    # Create dbt_project.yml
    cat > dbt_project.yml << 'DBTEOF'
name: 'ecommerce_elt'
version: '1.0.0'
profile: 'ecommerce_elt'
model-paths: ["models"]
target-path: "target"
models:
  ecommerce_elt:
    materialized: table
DBTEOF
    
    # Create profiles.yml
    cat > profiles.yml << 'PROFILESEOF'
ecommerce_elt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: admin
      password: password123
      port: 5432
      dbname: ecommerce
      schema: public
      threads: 4
PROFILESEOF
    
    # Create models directory and analytics models
    mkdir -p models
    
    # Customer analytics model
    cat > models/customer_analytics.sql << 'MODELEOF'
SELECT 
    customerid,
    country,
    COUNT(DISTINCT invoiceno) as total_orders,
    COUNT(*) as total_items,
    SUM(quantity * unitprice) as total_spent,
    AVG(quantity * unitprice) as avg_order_value,
    MIN(invoicedate) as first_purchase,
    MAX(invoicedate) as last_purchase
FROM ecommerce_data 
WHERE customerid IS NOT NULL AND quantity > 0
GROUP BY customerid, country
ORDER BY total_spent DESC
MODELEOF
    
    # Product performance model  
    cat > models/product_performance.sql << 'MODELEOF'
SELECT 
    stockcode,
    description,
    SUM(quantity) as total_units_sold,
    SUM(quantity * unitprice) as total_revenue,
    COUNT(DISTINCT customerid) as unique_customers,
    AVG(unitprice) as avg_price
FROM ecommerce_data 
WHERE quantity > 0
GROUP BY stockcode, description
ORDER BY total_revenue DESC
MODELEOF
    
    # Run dbt
    dbt debug --profiles-dir . &&
    dbt run --profiles-dir . &&
    echo "✅ dbt transformations completed successfully!"
    ''',
    dag=dag
)

# Set task dependencies
extract_load >> setup_and_run_dbt