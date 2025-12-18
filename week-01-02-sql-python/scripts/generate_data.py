# File: week-01-02-sql-python/scripts/generate_data.py

import psycopg2
from faker import Faker
import random
import os
from datetime import date, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ============================================================================
# FIXED SEED FOR DETERMINISTIC DATA GENERATION
# Ensures that every time you clear data and regenerate, 
# you get the exact same dataset
# ============================================================================
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)
fake = Faker()

# Fixed date range for 2025
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days + 1  # 365 days

def get_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'data_engineer'),
        user=os.getenv('POSTGRES_USER', 'dataengineer'),
        password=os.getenv('POSTGRES_PASSWORD', 'dataengineer123')
    )

def check_existing_data(conn):
    """Check if data already exists"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM analytics.customers;")
    count = cur.fetchone()[0]
    cur.close()
    
    if count > 0:
        print(f"\n{'='*70}")
        print(f"⚠️  DATA ALREADY EXISTS!")
        print(f"{'='*70}")
        print(f"\nFound {count:,} customers in database")
        print("\nOptions:")
        print("  1. Keep existing data (recommended)")
        print("  2. Delete and regenerate all data")
        print(f"\n{'='*70}")
        
        response = input("\nYour choice [1/2]: ").strip()
        
        if response == '2':
            print(f"\n{'='*70}")
            print("🗑️  DELETING ALL DATA")
            print(f"{'='*70}")
            cur = conn.cursor()
            cur.execute("TRUNCATE TABLE analytics.order_items CASCADE;")
            cur.execute("TRUNCATE TABLE analytics.orders CASCADE;")
            cur.execute("TRUNCATE TABLE analytics.customers CASCADE;")
            cur.execute("TRUNCATE TABLE analytics.products CASCADE;")
            cur.execute("TRUNCATE TABLE analytics.categories CASCADE;")
            conn.commit()
            cur.close()
            print("✅ All data deleted")
            return True
        else:
            print("\n✅ Keeping existing data")
            return False
    
    return True

def generate_customers(conn, num_customers):
    """Generate customer data"""
    print(f"\n📊 Generating {num_customers:,} customers...")
    cur = conn.cursor()
    
    customers = []
    emails = set()
    
    for _ in tqdm(range(num_customers), desc="Customers"):
        email = fake.email()
        while email in emails:
            email = fake.email()
        emails.add(email)
        
        country = fake.country()[:100]
        
        # Use deterministic date within 2025 based on customer index
        signup_offset = random.randint(0, TOTAL_DAYS - 1)
        signup_date = START_DATE + timedelta(days=signup_offset)
        
        customer = (
            fake.name()[:200],
            email[:200],
            country,
            signup_date,
            random.choice(['Premium', 'Standard', 'Basic'])
        )
        customers.append(customer)
    
    cur.executemany("""
        INSERT INTO analytics.customers 
        (customer_name, email, country, signup_date, customer_segment)
        VALUES (%s, %s, %s, %s, %s)
    """, customers)
    
    conn.commit()
    print(f"✅ Generated {num_customers:,} customers")
    cur.close()

def generate_products(conn, num_products):
    """Generate product data"""
    print(f"\n📦 Generating {num_products:,} products...")
    cur = conn.cursor()
    
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Sports', 'Toys']
    products = []
    
    for _ in tqdm(range(num_products), desc="Products"):
        cost = round(random.uniform(10, 500), 2)
        price = round(cost * random.uniform(1.3, 2.5), 2)
        product = (
            fake.catch_phrase()[:200],
            random.choice(categories),
            price,
            cost
        )
        products.append(product)
    
    cur.executemany("""
        INSERT INTO analytics.products 
        (product_name, category, price, cost)
        VALUES (%s, %s, %s, %s)
    """, products)
    
    conn.commit()
    print(f"✅ Generated {num_products:,} products")
    cur.close()

def generate_orders(conn, num_orders):
    """Generate orders and order items"""
    print(f"\n🛒 Generating {num_orders:,} orders...")
    cur = conn.cursor()
    
    # Get customer and product IDs
    cur.execute("SELECT customer_id FROM analytics.customers")
    customer_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT product_id, price FROM analytics.products")
    products = {row[0]: float(row[1]) for row in cur.fetchall()}
    product_ids = list(products.keys())
    
    batch_size = 1000
    total_batches = (num_orders + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, num_orders)
        batch_count = batch_end - batch_start
        
        orders = []
        order_items = []
        
        for i in tqdm(range(batch_count), desc=f"Batch {batch_num + 1}/{total_batches}"):
            order_id = batch_start + i + 1
            customer_id = random.choice(customer_ids)
            # Use deterministic date within 2025 based on order index
            order_offset = random.randint(0, TOTAL_DAYS - 1)
            order_date = START_DATE + timedelta(days=order_offset)
            status = random.choices(
                ['completed', 'pending', 'cancelled', 'returned'],
                weights=[0.7, 0.15, 0.1, 0.05]
            )[0]
            
            # Generate order items
            num_items = random.randint(1, 5)
            total = 0.0
            
            for _ in range(num_items):
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 5)
                unit_price = products[product_id]
                discount = random.choice([0, 5, 10, 15, 20])
                
                item_total = quantity * unit_price * (1 - discount/100.0)
                total += item_total
                
                order_items.append((
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount
                ))
            
            orders.append((
                customer_id,
                order_date,
                status,
                round(total, 2)
            ))
        
        # Insert batch
        cur.executemany("""
            INSERT INTO analytics.orders 
            (customer_id, order_date, order_status, total_amount)
            VALUES (%s, %s, %s, %s)
        """, orders)
        
        cur.executemany("""
            INSERT INTO analytics.order_items 
            (order_id, product_id, quantity, unit_price, discount_percent)
            VALUES (%s, %s, %s, %s, %s)
        """, order_items)
        
        conn.commit()
    
    print(f"✅ Generated {num_orders:,} orders")
    cur.close()

def generate_categories(conn):
    """Generate category hierarchy"""
    print("\n🏷️  Generating category hierarchy...")
    cur = conn.cursor()
    
    categories = [
        ('All Products', None),
        ('Electronics', 1),
        ('Computers', 2),
        ('Laptops', 3),
        ('Desktops', 3),
        ('Mobile Devices', 2),
        ('Smartphones', 6),
        ('Tablets', 6),
        ('Clothing', 1),
        ('Men', 9),
        ('Women', 9),
        ('Kids', 9),
    ]
    
    cur.executemany("""
        INSERT INTO analytics.categories (category_name, parent_category_id)
        VALUES (%s, %s)
    """, categories)
    
    conn.commit()
    print(f"✅ Generated {len(categories)} categories")
    cur.close()

def print_statistics(conn):
    """Print data statistics"""
    print(f"\n{'='*70}")
    print("📈 DATA GENERATION SUMMARY")
    print(f"{'='*70}")
    
    cur = conn.cursor()
    
    # Basic counts
    cur.execute("SELECT COUNT(*) FROM analytics.customers")
    customers_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM analytics.products")
    products_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM analytics.orders")
    orders_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM analytics.order_items")
    items_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM analytics.categories")
    categories_count = cur.fetchone()[0]
    
    # Revenue
    cur.execute("""
        SELECT 
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM analytics.orders 
        WHERE order_status = 'completed'
    """)
    revenue_data = cur.fetchone()
    total_revenue = float(revenue_data[0] or 0)
    avg_order_value = float(revenue_data[1] or 0)
    
    print(f"\n📊 Record Counts:")
    print(f"   Customers............ {customers_count:>10,}")
    print(f"   Products............. {products_count:>10,}")
    print(f"   Orders............... {orders_count:>10,}")
    print(f"   Order Items.......... {items_count:>10,}")
    print(f"   Categories........... {categories_count:>10,}")
    
    print(f"\n💰 Revenue Metrics:")
    print(f"   Total Revenue........ ${total_revenue:>10,.2f}")
    print(f"   Avg Order Value...... ${avg_order_value:>10,.2f}")
    
    # Date range
    cur.execute("""
        SELECT MIN(order_date), MAX(order_date) 
        FROM analytics.orders
    """)
    date_range = cur.fetchone()
    print(f"\n📅 Date Range:")
    print(f"   From................. {date_range[0]}")
    print(f"   To................... {date_range[1]}")
    
    # Top categories
    cur.execute("""
        SELECT 
            p.category,
            COUNT(*) as order_count,
            SUM(oi.quantity) as total_quantity,
            SUM(oi.line_total) as total_revenue
        FROM analytics.order_items oi
        JOIN analytics.products p ON oi.product_id = p.product_id
        JOIN analytics.orders o ON oi.order_id = o.order_id
        WHERE o.order_status = 'completed'
        GROUP BY p.category
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    
    print(f"\n🏆 Top 5 Categories by Revenue:")
    for row in cur.fetchall():
        category, order_count, quantity, revenue = row
        revenue = float(revenue or 0)
        print(f"   {category:.<20} ${revenue:>12,.2f} ({order_count:,} orders)")
    
    print(f"\n{'='*70}")
    cur.close()

def main():
    """Main execution"""
    print(f"\n{'='*70}")
    print("🚀 DATA GENERATION SCRIPT")
    print(f"{'='*70}")
    
    try:
        conn = get_connection()
        print("✅ Connected to database: data_engineer")
        
        # Check existing data
        should_generate = check_existing_data(conn)
        
        if not should_generate:
            print_statistics(conn)
            conn.close()
            return
        
        # Get settings from environment
        num_customers = int(os.getenv('GENERATE_CUSTOMERS', 1000))
        num_products = int(os.getenv('GENERATE_PRODUCTS', 100))
        num_orders = int(os.getenv('GENERATE_ORDERS', 10000))
        
        print(f"\n📋 Generation Settings:")
        print(f"   Customers: {num_customers:,}")
        print(f"   Products: {num_products:,}")
        print(f"   Orders: {num_orders:,}")
        
        # Generate data
        generate_customers(conn, num_customers)
        generate_products(conn, num_products)
        generate_categories(conn)
        generate_orders(conn, num_orders)
        
        # Print statistics
        print_statistics(conn)
        
        conn.close()
        
        print(f"\n{'='*70}")
        print("✅ DATA GENERATION COMPLETED SUCCESSFULLY!")
        print(f"{'='*70}")
        print("\nNext steps:")
        print("  1. Open PgAdmin: http://localhost:5050")
        print("  2. Open Jupyter: http://localhost:8888")
        print("  3. Start practicing SQL queries!")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"\n{'='*70}")
        print("❌ ERROR OCCURRED!")
        print(f"{'='*70}")
        print(f"\n{e}\n")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()