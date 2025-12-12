# File: week-01-02-sql-python/scripts/test_connection.py

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    """Test PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            database=os.getenv('POSTGRES_DB', 'data_engineer'),
            user=os.getenv('POSTGRES_USER', 'dataengineer'),
            password=os.getenv('POSTGRES_PASSWORD', 'dataengineer123')
        )
        
        cur = conn.cursor()
        
        # Test connection
        cur.execute('SELECT version();')
        version = cur.fetchone()
        print("=" * 60)
        print("✅ CONNECTION SUCCESSFUL!")
        print("=" * 60)
        print(f"\n📊 PostgreSQL version:\n{version[0]}\n")
        
        # Test database
        cur.execute('SELECT current_database();')
        db_name = cur.fetchone()[0]
        print(f"📁 Current database: {db_name}")
        
        # Test schemas
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('analytics', 'staging', 'raw')
            ORDER BY schema_name
        """)
        schemas = cur.fetchall()
        print(f"\n📂 Schemas found: {', '.join([s[0] for s in schemas])}")
        
        # Test tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'analytics'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"\n📋 Tables in analytics schema:")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Test setup_test table
        cur.execute("SELECT * FROM analytics.setup_test;")
        test_result = cur.fetchone()
        if test_result:
            print(f"\n✅ Setup test: {test_result[1]}")
        
        # Count records
        if 'customers' in [t[0] for t in tables]:
            cur.execute("SELECT COUNT(*) FROM analytics.customers;")
            count = cur.fetchone()[0]
            print(f"\n📊 Sample data:")
            print(f"   - Customers: {count:,}")
            
            if count > 0:
                cur.execute("SELECT COUNT(*) FROM analytics.products;")
                print(f"   - Products: {cur.fetchone()[0]:,}")
                
                cur.execute("SELECT COUNT(*) FROM analytics.orders;")
                print(f"   - Orders: {cur.fetchone()[0]:,}")
                
                cur.execute("SELECT COUNT(*) FROM analytics.order_items;")
                print(f"   - Order Items: {cur.fetchone()[0]:,}")
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED!")
        print("=" * 60)
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ CONNECTION FAILED!")
        print("=" * 60)
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Check if PostgreSQL container is running: docker ps")
        print("2. Check logs: docker logs de-postgres")
        print("3. Verify .env file settings")
        print("=" * 60)
        return False

if __name__ == "__main__":
    test_connection()