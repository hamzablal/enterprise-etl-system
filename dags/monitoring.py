# dags/monitoring.py - Store metrics in PostgreSQL
from sqlalchemy import create_engine, text
from datetime import datetime

def log_pipeline_metrics(status, records_processed=0, duration=0, error_message=None):
    """Log pipeline metrics to PostgreSQL"""
    engine = create_engine('postgresql://admin:password123@postgres:5432/ecommerce')
    
    # Create metrics table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pipeline_metrics (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        pipeline_name VARCHAR(100),
        status VARCHAR(20),
        records_processed INTEGER,
        duration_seconds FLOAT,
        error_message TEXT
    );
    """
    
    # Insert metrics
    insert_sql = """
    INSERT INTO pipeline_metrics 
    (pipeline_name, status, records_processed, duration_seconds, error_message)
    VALUES (:pipeline_name, :status, :records_processed, :duration, :error_message)
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.execute(text(insert_sql), {
                'pipeline_name': 'complete_elt_pipeline',
                'status': status,
                'records_processed': records_processed,
                'duration': duration,
                'error_message': error_message
            })
            conn.commit()
        print(f"📊 Logged metrics: {status}, {records_processed} records, {duration}s")
    except Exception as e:
        print(f"⚠️ Failed to log metrics: {e}")

def log_pipeline_start():
    """Log pipeline start"""
    log_pipeline_metrics('STARTED')

def log_pipeline_success(records_processed, duration):
    """Log successful pipeline completion"""
    log_pipeline_metrics('SUCCESS', records_processed, duration)

def log_pipeline_failure(error_message):
    """Log pipeline failure"""
    log_pipeline_metrics('FAILED', error_message=str(error_message))