import psycopg2
from datetime import datetime
import json

def check_pipeline_health():
    """Monitor pipeline and generate report"""
    
    # Connect to database
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='ecommerce',
        user='admin',
        password='password123'
    )
    cursor = conn.cursor()
    
    # Get data metrics
    cursor.execute("SELECT COUNT(*) FROM ecommerce_data")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT customerid) FROM ecommerce_data WHERE customerid IS NOT NULL")
    unique_customers = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(quantity * unitprice) FROM ecommerce_data WHERE quantity > 0")
    total_revenue = cursor.fetchone()[0]
    
    # Generate report
    report = {
        'timestamp': datetime.now().isoformat(),
        'pipeline_status': 'SUCCESS',
        'data_quality': {
            'total_records': total_records,
            'unique_customers': unique_customers,
            'total_revenue': float(total_revenue) if total_revenue else 0
        }
    }
    
    # Save report
    with open('pipeline_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"✅ Pipeline Health Check:")
    print(f"   Records: {total_records:,}")
    print(f"   Customers: {unique_customers:,}")
    print(f"   Revenue: ${total_revenue:,.2f}" if total_revenue else "   Revenue: $0.00")
    
    conn.close()

if __name__ == "__main__":
    check_pipeline_health()