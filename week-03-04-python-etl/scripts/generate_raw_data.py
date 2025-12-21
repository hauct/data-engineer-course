"""
Generate Raw Data for ETL Pipeline
Creates partitioned Parquet files with intentional data quality issues for learning

Output Structure:
    raw_data/
    ├── customers/
    │   ├── 2025-01-01/data.parquet
    │   └── ...
    ├── products/
    │   ├── 2025-01-01/data.parquet
    │   └── ...
    ├── orders/
    │   ├── 2025-01-01/data.parquet
    │   └── ...
    └── order_items/
        ├── 2025-01-01/data.parquet
        └── ...
"""

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import date, timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import argparse
import os

# =============================================================================
# FIXED SEED FOR DETERMINISTIC DATA GENERATION
# Ensures that every time you clear data and regenerate,
# you get the exact same dataset
# =============================================================================
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)
fake = Faker()

# Fixed date range for 2025
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days + 1  # 365 days

# Data generation parameters
CUSTOMERS_PER_DAY_RANGE = (10, 50)
ORDERS_PER_DAY_RANGE = (100, 500)
ITEMS_PER_ORDER_RANGE = (1, 5)

# Error rates (for learning purposes)
DUPLICATE_NAME_RATE = 0.04  # 4% duplicate names (same person, different email)
NULL_NAME_RATE = 0.02       # 2% null names
NULL_COUNTRY_RATE = 0.01    # 1% null country
LOWERCASE_NAME_RATE = 0.10  # 10% lowercase names (to fix in STG)
INVALID_EMAIL_RATE = 0.02   # 2% invalid emails (but still unique)

# Categories and Products
CATEGORIES = ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Sports', 'Toys']
CUSTOMER_SEGMENTS = ['Premium', 'Standard', 'Basic']
ORDER_STATUSES = ['completed', 'pending', 'cancelled', 'returned']
ORDER_STATUS_WEIGHTS = [0.7, 0.15, 0.1, 0.05]


def get_raw_data_dir():
    """Get the raw_data directory path"""
    script_dir = Path(__file__).parent
    return script_dir.parent / 'raw_data'


def introduce_errors_customer(customer: dict, error_rng: random.Random) -> dict:
    """
    Introduce intentional errors into customer data
    
    Rules:
    - email: ALWAYS unique, NEVER null (enforced in generation)
    - customer_name: Can be null (2% chance), can be lowercase (10% chance)
    - country: Can be null (1% chance)
    """
    # ✅ Lowercase name (10% chance) - only if name is not null
    if customer['customer_name'] is not None and error_rng.random() < LOWERCASE_NAME_RATE:
        customer['customer_name'] = customer['customer_name'].lower()
    
    # ✅ Null country (1% chance)
    if error_rng.random() < NULL_COUNTRY_RATE:
        customer['country'] = None
    
    # ✅ Invalid email format (2% chance) - but still unique
    if error_rng.random() < INVALID_EMAIL_RATE:
        customer['email'] = customer['email'].replace('@', '_at_')
    
    return customer


def introduce_errors_product(product: dict, error_rng: random.Random) -> dict:
    """Introduce intentional errors into product data"""
    # Lowercase product name (10% chance)
    if error_rng.random() < LOWERCASE_NAME_RATE:
        product['product_name'] = product['product_name'].lower()
    
    return product


def generate_products_master() -> pd.DataFrame:
    """Generate master product list (100 products)"""
    products = []
    for product_id in range(1, 101):
        cost = round(random.uniform(10, 500), 2)
        price = round(cost * random.uniform(1.3, 2.5), 2)
        products.append({
            'product_id': product_id,
            'product_name': fake.catch_phrase()[:200],
            'category': random.choice(CATEGORIES),
            'price': price,
            'cost': cost
        })
    return pd.DataFrame(products)


def generate_customers_for_day(
    day: date,
    start_customer_id: int,
    error_rng: random.Random,
    global_emails: set = None
) -> pd.DataFrame:
    """
    Generate new customers for a specific day
    
    Ensures:
    - customer_id: ALWAYS unique (primary key)
    - email: ALWAYS unique, NEVER null (business key)
    - customer_name: Can be NULL (2% chance), can be duplicate (4% chance)
    - country: Can be NULL (1% chance)
    """
    if global_emails is None:
        global_emails = set()
    
    num_customers = random.randint(*CUSTOMERS_PER_DAY_RANGE)
    customers = []
    emails_used = global_emails.copy()  # ✅ Start with global emails
    customer_ids_used = set()
    null_name_count = 0
    duplicate_name_count = 0
    
    # Generate base customers
    for i in range(num_customers):
        customer_id = start_customer_id + i
        customer_ids_used.add(customer_id)
        
        # ✅ Generate UNIQUE email (check against global set)
        email = fake.email()
        attempt = 0
        while email in emails_used and attempt < 1000:
            email = fake.email()
            attempt += 1
        
        # Fallback: if still duplicate after 1000 attempts
        if email in emails_used:
            email = f"{fake.user_name()}_{customer_id}_{day.strftime('%Y%m%d')}@{fake.domain_name()}"
        
        emails_used.add(email)
        
        # ✅ Generate customer_name (can be NULL with 2% chance)
        if error_rng.random() < NULL_NAME_RATE:
            customer_name = None
            null_name_count += 1
        else:
            customer_name = fake.name()[:200]
        
        customer = {
            'customer_id': customer_id,
            'customer_name': customer_name,
            'email': email[:200],
            'country': fake.country()[:100],
            'signup_date': day,
            'customer_segment': random.choice(CUSTOMER_SEGMENTS)
        }
        
        # Introduce errors (lowercase, null country, invalid email format)
        customer = introduce_errors_customer(customer, error_rng)
        customers.append(customer)
    
    # ✅ Add "duplicate people" (same name/country, different ID/email)
    # This simulates: same person signing up multiple times
    num_duplicates = int(len(customers) * DUPLICATE_NAME_RATE)
    if num_duplicates > 0 and customers:
        next_id = start_customer_id + len(customers)
        
        for i in range(num_duplicates):
            # Pick a random customer to duplicate (skip if name is null)
            original = random.choice(customers)
            if original['customer_name'] is None:
                continue
            
            # Generate NEW unique customer_id
            new_customer_id = next_id + i
            while new_customer_id in customer_ids_used:
                new_customer_id += 1
            customer_ids_used.add(new_customer_id)
            
            # Generate NEW unique email
            new_email = fake.email()
            attempt = 0
            while new_email in emails_used and attempt < 1000:
                new_email = fake.email()
                attempt += 1
            
            if new_email in emails_used:
                new_email = f"{fake.user_name()}_{new_customer_id}_{random.randint(1000, 9999)}@{fake.domain_name()}"
            
            emails_used.add(new_email)
            
            # Create duplicate with SAME name/country but UNIQUE id/email
            duplicate = {
                'customer_id': new_customer_id,              # ✅ UNIQUE
                'customer_name': original['customer_name'],  # ❌ DUPLICATE (intentional)
                'email': new_email[:200],                    # ✅ UNIQUE
                'country': original['country'],              # ❌ DUPLICATE (intentional)
                'signup_date': day,
                'customer_segment': original['customer_segment']
            }
            
            # Apply errors to duplicate too
            duplicate = introduce_errors_customer(duplicate, error_rng)
            customers.append(duplicate)
            duplicate_name_count += 1
    
    df = pd.DataFrame(customers) if customers else pd.DataFrame()
    
    # ✅ VERIFY: No duplicate IDs or emails
    if not df.empty:
        unique_ids = df['customer_id'].nunique()
        unique_emails = df['email'].nunique()
        total_records = len(df)
        
        assert unique_ids == total_records, f"❌ Duplicate customer_id! Expected {total_records}, got {unique_ids}"
        assert unique_emails == total_records, f"❌ Duplicate email! Expected {total_records}, got {unique_emails}"
        assert df['email'].isna().sum() == 0, "❌ Found NULL emails!"
    
    return df


def generate_orders_for_day(
    day: date,
    start_order_id: int,
    customer_ids: list,
    products_df: pd.DataFrame,
    error_rng: random.Random
) -> tuple:
    """Generate orders and order items for a specific day"""
    if not customer_ids:
        return pd.DataFrame(), pd.DataFrame()
    
    num_orders = random.randint(*ORDERS_PER_DAY_RANGE)
    orders = []
    order_items = []
    order_item_id = start_order_id * 10  # Simple way to generate unique IDs
    
    product_ids = products_df['product_id'].tolist()
    product_prices = products_df.set_index('product_id')['price'].to_dict()
    
    for i in range(num_orders):
        order_id = start_order_id + i
        customer_id = random.choice(customer_ids)
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
        
        # Generate order items
        num_items = random.randint(*ITEMS_PER_ORDER_RANGE)
        order_total = 0.0
        
        for j in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            unit_price = product_prices[product_id]
            discount = random.choice([0, 5, 10, 15, 20])
            line_total = quantity * unit_price * (1 - discount / 100.0)
            order_total += line_total
            
            order_items.append({
                'order_item_id': order_item_id,
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percent': discount
            })
            order_item_id += 1
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': day,
            'order_status': status,
            'total_amount': round(order_total, 2)
        })
    
    # Add duplicates to orders (4%)
    num_dup_orders = int(len(orders) * 0.04)
    if num_dup_orders > 0:
        dup_orders = random.choices(orders, k=num_dup_orders)
        orders.extend(dup_orders)
    
    # Add duplicates to order_items (4%)
    num_dup_items = int(len(order_items) * 0.04)
    if num_dup_items > 0:
        dup_items = random.choices(order_items, k=num_dup_items)
        order_items.extend(dup_items)
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)


def save_parquet(df: pd.DataFrame, output_path: Path):
    """Save DataFrame as Parquet file"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine='pyarrow', index=False)


def generate_all_data(output_dir: Path, test_mode: bool = False):
    """Generate all raw data for the entire year"""
    
    # Use test mode for quick testing (only 3 days)
    if test_mode:
        end_date = START_DATE + timedelta(days=2)
        date_range = [START_DATE + timedelta(days=i) for i in range(3)]
        print(f"\n[TEST MODE] Generating data for {len(date_range)} days only\n")
    else:
        date_range = [START_DATE + timedelta(days=i) for i in range(TOTAL_DAYS)]
        print(f"\n[FULL MODE] Generating data for {TOTAL_DAYS} days\n")
    
    # Error RNG (separate from main RNG for reproducibility)
    error_rng = random.Random(RANDOM_SEED + 1)
    
    # Generate master products (same for all days)
    print("Generating master product catalog...")
    products_df = generate_products_master()
    
    # Introduce errors to products
    for idx, row in products_df.iterrows():
        products_df.loc[idx] = pd.Series(
            introduce_errors_product(row.to_dict(), error_rng)
        )
    
    # Tracking
    all_customer_ids = []
    all_emails = set()  # ✅ Track emails globally across all days
    current_customer_id = 1
    current_order_id = 1
    
    # Stats
    total_customers = 0
    total_unique_emails = 0
    total_null_names = 0
    total_orders = 0
    total_items = 0
    
    print("\nGenerating daily partitioned data...")
    for day in tqdm(date_range, desc="Days"):
        day_str = day.strftime('%Y-%m-%d')
        
        # Generate customers for this day
        customers_df = generate_customers_for_day(
            day, current_customer_id, error_rng, all_emails  # ✅ Pass emails
        )
        
        if not customers_df.empty:
            # Save customers
            save_parquet(
                customers_df,
                output_dir / 'customers' / day_str / 'data.parquet'
            )

            # ✅ Update global email set
            new_emails = set(customers_df['email'].dropna().unique())
            all_emails.update(new_emails)
            
            # Track statistics
            new_ids = customers_df['customer_id'].unique().tolist()
            all_customer_ids.extend(new_ids)
            
            new_emails = set(customers_df['email'].dropna().unique())
            all_emails.update(new_emails)
            
            null_names = customers_df['customer_name'].isna().sum()
            total_null_names += null_names
            
            current_customer_id += len(customers_df)
            total_customers += len(customers_df)
        
        # Save products (same data each day for simplicity)
        save_parquet(
            products_df.copy(),
            output_dir / 'products' / day_str / 'data.parquet'
        )
        
        # Generate orders (need some customers first)
        if all_customer_ids and day >= START_DATE + timedelta(days=1):
            orders_df, order_items_df = generate_orders_for_day(
                day, current_order_id, all_customer_ids, products_df, error_rng
            )
            
            if not orders_df.empty:
                save_parquet(
                    orders_df,
                    output_dir / 'orders' / day_str / 'data.parquet'
                )
                save_parquet(
                    order_items_df,
                    output_dir / 'order_items' / day_str / 'data.parquet'
                )
                
                current_order_id += len(orders_df)
                total_orders += len(orders_df)
                total_items += len(order_items_df)
    
    total_unique_emails = len(all_emails)
    
    # Print summary
    print(f"\n{'='*60}")
    print("RAW DATA GENERATION COMPLETE")
    print(f"{'='*60}")
    print(f"Output directory: {output_dir}")
    print(f"Date range: {START_DATE} to {date_range[-1]}")
    print(f"Days generated: {len(date_range)}")
    print(f"\nRecords generated:")
    print(f"  Customers:   {total_customers:,}")
    print(f"  Products:    {len(products_df):,} (same each day)")
    print(f"  Orders:      {total_orders:,}")
    print(f"  Order Items: {total_items:,}")
    print(f"\n✅ Data Quality Metrics:")
    print(f"  Unique Emails:       {total_unique_emails:,} (100% of customers)")
    print(f"  NULL Names:          {total_null_names:,} ({total_null_names/total_customers*100:.1f}%)")
    print(f"  Duplicate Names:     ~{int(total_customers * DUPLICATE_NAME_RATE):,} ({DUPLICATE_NAME_RATE*100:.0f}%)")
    print(f"\nError rates applied:")
    print(f"  Duplicate names:     {DUPLICATE_NAME_RATE*100:.0f}%")
    print(f"  NULL names:          {NULL_NAME_RATE*100:.0f}%")
    print(f"  NULL country:        {NULL_COUNTRY_RATE*100:.0f}%")
    print(f"  Lowercase names:     {LOWERCASE_NAME_RATE*100:.0f}%")
    print(f"  Invalid email format:{INVALID_EMAIL_RATE*100:.0f}%")
    print(f"{'='*60}\n")


def clear_raw_data(output_dir: Path):
    """Clear all raw data"""
    import shutil
    if output_dir.exists():
        shutil.rmtree(output_dir)
        print(f"Cleared: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description='Generate raw data for ETL pipeline')
    parser.add_argument('--test-mode', action='store_true', 
                        help='Generate only 3 days of data for testing')
    parser.add_argument('--clear', action='store_true',
                        help='Clear existing raw data before generating')
    args = parser.parse_args()
    
    output_dir = get_raw_data_dir()
    
    if args.clear:
        clear_raw_data(output_dir)
    
    generate_all_data(output_dir, test_mode=args.test_mode)


if __name__ == '__main__':
    main()