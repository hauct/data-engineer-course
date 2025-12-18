"""
ETL Staging Layer - Transform from raw schema to stg schema

Features:
- Validate data (email format, data types)
- Remove duplicates
- Handle null values
- Standardize text (capitalize names)
- Convert data types
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Optional
import argparse
import re

from db_connector import DatabaseConnector
from data_cleaner import DataCleaner

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StagingLayerETL:
    """ETL for transforming raw data to staging schema"""
    
    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        self.db = db_connector or DatabaseConnector()
    
    def _capitalize_name(self, name: str) -> str:
        """Capitalize each word in a name"""
        if pd.isna(name):
            return name
        return ' '.join(word.capitalize() for word in str(name).split())
    
    def _validate_email(self, email: str) -> bool:
        """Validate email format"""
        if pd.isna(email):
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))
    
    def transform_customers(self) -> dict:
        """Transform customers from raw to stg"""
        logger.info("Transforming customers...")
        
        # Extract from raw
        query = """
        SELECT customer_id, customer_name, email, country, signup_date, customer_segment
        FROM raw.customers
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            logger.warning("No customers in raw schema")
            return {'table': 'customers', 'rows': 0}
        
        original_count = len(df)
        
        # Remove duplicates (keep first occurrence)
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        dups_removed = original_count - len(df)
        
        # Capitalize names
        df['customer_name'] = df['customer_name'].apply(self._capitalize_name)
        
        # Validate and fix emails
        df['email_valid'] = df['email'].apply(self._validate_email)
        invalid_emails = (~df['email_valid']).sum()
        
        # Remove rows with invalid emails
        df = df[df['email_valid']].drop(columns=['email_valid'])
        
        # Handle nulls
        df = df.dropna(subset=['customer_id', 'customer_name', 'email', 'signup_date'])
        nulls_removed = original_count - dups_removed - len(df)
        
        # Fill optional nulls
        df['country'] = df['country'].fillna('Unknown')
        df['customer_segment'] = df['customer_segment'].fillna('Standard')
        
        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Load to staging (replace mode for full refresh)
        self.db.truncate_table('customers', schema='staging')
        rows = self.db.write_dataframe(df, 'customers', schema='staging', if_exists='append')
        
        logger.info(f"Customers: {original_count} raw -> {rows} stg")
        logger.info(f"  Duplicates removed: {dups_removed}")
        logger.info(f"  Invalid emails removed: {invalid_emails}")
        logger.info(f"  Nulls removed: {nulls_removed}")
        
        return {'table': 'customers', 'rows': rows, 'dups_removed': dups_removed, 'nulls_removed': nulls_removed}
    
    def transform_products(self) -> dict:
        """Transform products from raw to stg"""
        logger.info("Transforming products...")
        
        query = """
        SELECT DISTINCT product_id, product_name, category, price, cost
        FROM raw.products
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            logger.warning("No products in raw schema")
            return {'table': 'products', 'rows': 0}
        
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['product_id'], keep='first')
        
        # Capitalize product names
        df['product_name'] = df['product_name'].apply(self._capitalize_name)
        
        # Ensure positive values
        df = df[(df['price'] >= 0) & (df['cost'] >= 0)]
        
        # Remove nulls
        df = df.dropna(subset=['product_id', 'product_name', 'category', 'price', 'cost'])
        
        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Load to staging
        self.db.truncate_table('products', schema='staging')
        rows = self.db.write_dataframe(df, 'products', schema='staging', if_exists='append')
        
        logger.info(f"Products: {original_count} raw -> {rows} stg")
        return {'table': 'products', 'rows': rows}
    
    def transform_orders(self) -> dict:
        """Transform orders from raw to stg"""
        logger.info("Transforming orders...")
        
        # Get valid customer IDs from staging
        valid_customers = set(self.db.read_sql(
            "SELECT customer_id FROM staging.customers"
        )['customer_id'].tolist())
        
        query = """
        SELECT order_id, customer_id, order_date, order_status, total_amount
        FROM raw.orders
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            logger.warning("No orders in raw schema")
            return {'table': 'orders', 'rows': 0}
        
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        
        # Keep only orders with valid customers (referential integrity)
        df = df[df['customer_id'].isin(valid_customers)]
        
        # Validate order status
        valid_statuses = ['completed', 'pending', 'cancelled', 'returned']
        df = df[df['order_status'].isin(valid_statuses)]
        
        # Ensure positive amounts
        df = df[df['total_amount'] >= 0]
        
        # Remove nulls
        df = df.dropna(subset=['order_id', 'customer_id', 'order_date', 'order_status', 'total_amount'])
        
        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Load to staging
        self.db.truncate_table('orders', schema='staging')
        rows = self.db.write_dataframe(df, 'orders', schema='staging', if_exists='append')
        
        logger.info(f"Orders: {original_count} raw -> {rows} stg")
        return {'table': 'orders', 'rows': rows}
    
    def transform_order_items(self) -> dict:
        """Transform order_items from raw to stg"""
        logger.info("Transforming order_items...")
        
        # Get valid order and product IDs
        valid_orders = set(self.db.read_sql(
            "SELECT order_id FROM staging.orders"
        )['order_id'].tolist())
        
        valid_products = set(self.db.read_sql(
            "SELECT product_id FROM staging.products"
        )['product_id'].tolist())
        
        query = """
        SELECT order_item_id, order_id, product_id, quantity, unit_price, discount_percent
        FROM raw.order_items
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            logger.warning("No order_items in raw schema")
            return {'table': 'order_items', 'rows': 0}
        
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['order_item_id'], keep='first')
        
        # Referential integrity
        df = df[df['order_id'].isin(valid_orders)]
        df = df[df['product_id'].isin(valid_products)]
        
        # Validate values
        df = df[(df['quantity'] > 0) & (df['unit_price'] >= 0)]
        df = df[(df['discount_percent'] >= 0) & (df['discount_percent'] <= 100)]
        
        # Remove nulls
        df = df.dropna(subset=['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price'])
        
        # Fill default discount
        df['discount_percent'] = df['discount_percent'].fillna(0)
        
        # Add timestamp
        df['created_at'] = datetime.now()
        
        # Load to staging
        self.db.truncate_table('order_items', schema='staging')
        rows = self.db.write_dataframe(df, 'order_items', schema='staging', if_exists='append')
        
        logger.info(f"Order Items: {original_count} raw -> {rows} stg")
        return {'table': 'order_items', 'rows': rows}
    
    def transform_all(self) -> dict:
        """Transform all tables from raw to staging"""
        logger.info("="*60)
        logger.info("ETL STAGING LAYER - STARTING")
        logger.info("="*60)
        
        results = {}
        
        # Order matters due to foreign key dependencies
        results['customers'] = self.transform_customers()
        results['products'] = self.transform_products()
        results['orders'] = self.transform_orders()
        results['order_items'] = self.transform_order_items()
        
        logger.info("\n" + "="*60)
        logger.info("ETL STAGING LAYER - COMPLETE")
        logger.info("="*60)
        
        total_rows = sum(r['rows'] for r in results.values())
        logger.info(f"Total rows in staging: {total_rows:,}")
        
        return results


def main():
    parser = argparse.ArgumentParser(description='ETL Staging Layer')
    parser.add_argument('--table', type=str,
                        help='Process specific table only')
    args = parser.parse_args()
    
    etl = StagingLayerETL()
    
    if args.table:
        method_name = f"transform_{args.table}"
        if hasattr(etl, method_name):
            result = getattr(etl, method_name)()
            print(f"\nResult: {result}")
        else:
            print(f"Unknown table: {args.table}")
    else:
        results = etl.transform_all()
        print(f"\nResults: {results}")


if __name__ == '__main__':
    main()
