# File: week-01-02-sql-python/scripts/reset_data.py

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'data_engineer'),
        user=os.getenv('POSTGRES_USER', 'dataengineer'),
        password=os.getenv('POSTGRES_PASSWORD', 'dataengineer123')
    )

def reset_data():
    """Reset all data in database"""
    print(f"\n{'='*70}")
    print("🗑️  RESET DATA")
    print(f"{'='*70}")
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Get counts before deletion
        cur.execute("SELECT COUNT(*) FROM analytics.customers")
        customers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM analytics.orders")
        orders = cur.fetchone()[0]
        
        print(f"\nCurrent data:")
        print(f"  - Customers: {customers:,}")
        print(f"  - Orders: {orders:,}")
        
        print(f"\n{'='*70}")
        print("⚠️  WARNING: This will delete ALL data!")
        print(f"{'='*70}")
        
        # Truncate tables
        print("\nDeleting data...")
        cur.execute("TRUNCATE TABLE analytics.order_items CASCADE;")
        cur.execute("TRUNCATE TABLE analytics.orders CASCADE;")
        cur.execute("TRUNCATE TABLE analytics.customers CASCADE;")
        cur.execute("TRUNCATE TABLE analytics.products CASCADE;")
        cur.execute("TRUNCATE TABLE analytics.categories CASCADE;")
        
        conn.commit()
        
        print("✅ All data deleted successfully!")
        print("\nTo generate new data, run:")
        print("  make generate-data")
        print(f"\n{'='*70}\n")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        raise

if __name__ == "__main__":
    reset_data()