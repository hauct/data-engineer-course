"""
ETL Production Layer - Aggregate from stg schema to prod schema

Features:
- Daily and monthly sales aggregations
- Category metrics (revenue, customers per category)
- Product metrics (revenue, quantity per product)
- Customer lifetime metrics
"""

import pandas as pd
from datetime import datetime
import logging
from typing import Optional
import argparse

from db_connector import DatabaseConnector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProdLayerETL:
    """ETL for aggregating staging data to production metrics"""
    
    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        self.db = db_connector or DatabaseConnector()
    
    def build_daily_sales(self) -> dict:
        """Build daily sales summary"""
        logger.info("Building daily_sales...")
        
        query = """
        SELECT 
            o.order_date,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(oi.quantity) as total_items,
            SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as total_revenue,
            COUNT(DISTINCT o.customer_id) as total_customers
        FROM staging.orders o
        JOIN staging.order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status = 'completed'
        GROUP BY o.order_date
        ORDER BY o.order_date
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            logger.warning("No completed orders in staging")
            return {'table': 'daily_sales', 'rows': 0}
        
        # Calculate average order value
        df['avg_order_value'] = (df['total_revenue'] / df['total_orders']).round(2)
        df['total_revenue'] = df['total_revenue'].round(2)
        
        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Load to prod (replace)
        self.db.truncate_table('daily_sales', schema='prod')
        rows = self.db.write_dataframe(df, 'daily_sales', schema='prod', if_exists='append')
        
        logger.info(f"Daily Sales: {rows} days aggregated")
        return {'table': 'daily_sales', 'rows': rows}
    
    def build_monthly_sales(self) -> dict:
        """Build monthly sales summary"""
        logger.info("Building monthly_sales...")
        
        query = """
        SELECT 
            TO_CHAR(o.order_date, 'YYYY-MM') as year_month,
            EXTRACT(YEAR FROM o.order_date)::INTEGER as year,
            EXTRACT(MONTH FROM o.order_date)::INTEGER as month,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(oi.quantity) as total_items,
            SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as total_revenue,
            COUNT(DISTINCT o.customer_id) as total_customers
        FROM staging.orders o
        JOIN staging.order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status = 'completed'
        GROUP BY year_month, year, month
        ORDER BY year_month
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            return {'table': 'monthly_sales', 'rows': 0}
        
        df['avg_order_value'] = (df['total_revenue'] / df['total_orders']).round(2)
        df['total_revenue'] = df['total_revenue'].round(2)
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        self.db.truncate_table('monthly_sales', schema='prod')
        rows = self.db.write_dataframe(df, 'monthly_sales', schema='prod', if_exists='append')
        
        logger.info(f"Monthly Sales: {rows} months aggregated")
        return {'table': 'monthly_sales', 'rows': rows}
    
    def build_daily_category_metrics(self) -> dict:
        """Build daily category metrics"""
        logger.info("Building daily_category_metrics...")
        
        query = """
        SELECT 
            o.order_date,
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(oi.quantity) as total_items,
            SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as total_revenue,
            COUNT(DISTINCT o.customer_id) as unique_customers,
            COUNT(DISTINCT p.product_id) as unique_products
        FROM staging.orders o
        JOIN staging.order_items oi ON o.order_id = oi.order_id
        JOIN staging.products p ON oi.product_id = p.product_id
        WHERE o.order_status = 'completed'
        GROUP BY o.order_date, p.category
        ORDER BY o.order_date, p.category
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            return {'table': 'daily_category_metrics', 'rows': 0}
        
        df['total_revenue'] = df['total_revenue'].round(2)
        df['created_at'] = datetime.now()
        
        self.db.truncate_table('daily_category_metrics', schema='prod')
        rows = self.db.write_dataframe(df, 'daily_category_metrics', schema='prod', if_exists='append')
        
        logger.info(f"Daily Category Metrics: {rows} rows")
        return {'table': 'daily_category_metrics', 'rows': rows}
    
    def build_daily_product_metrics(self) -> dict:
        """Build daily product metrics"""
        logger.info("Building daily_product_metrics...")
        
        query = """
        SELECT 
            o.order_date,
            p.product_id,
            p.product_name,
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(oi.quantity) as total_quantity,
            SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as total_revenue,
            COUNT(DISTINCT o.customer_id) as unique_customers
        FROM staging.orders o
        JOIN staging.order_items oi ON o.order_id = oi.order_id
        JOIN staging.products p ON oi.product_id = p.product_id
        WHERE o.order_status = 'completed'
        GROUP BY o.order_date, p.product_id, p.product_name, p.category
        ORDER BY o.order_date, total_revenue DESC
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            return {'table': 'daily_product_metrics', 'rows': 0}
        
        df['total_revenue'] = df['total_revenue'].round(2)
        df['created_at'] = datetime.now()
        
        self.db.truncate_table('daily_product_metrics', schema='prod')
        rows = self.db.write_dataframe(df, 'daily_product_metrics', schema='prod', if_exists='append')
        
        logger.info(f"Daily Product Metrics: {rows} rows")
        return {'table': 'daily_product_metrics', 'rows': rows}
    
    def build_customer_metrics(self) -> dict:
        """Build customer lifetime metrics"""
        logger.info("Building customer_metrics...")
        
        query = """
        SELECT 
            c.customer_id,
            c.customer_name,
            c.customer_segment,
            MIN(o.order_date) as first_order_date,
            MAX(o.order_date) as last_order_date,
            COUNT(DISTINCT o.order_id) as total_orders,
            COALESCE(SUM(oi.quantity), 0) as total_items,
            COALESCE(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)), 0) as total_revenue
        FROM staging.customers c
        LEFT JOIN staging.orders o ON c.customer_id = o.customer_id AND o.order_status = 'completed'
        LEFT JOIN staging.order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_id, c.customer_name, c.customer_segment
        ORDER BY total_revenue DESC
        """
        df = self.db.read_sql(query)
        
        if df.empty:
            return {'table': 'customer_metrics', 'rows': 0}
        
        # Calculate derived metrics
        df['total_revenue'] = df['total_revenue'].round(2)
        df['avg_order_value'] = (df['total_revenue'] / df['total_orders'].replace(0, 1)).round(2)
        
        # Days calculations
        today = pd.Timestamp.now().date()
        df['first_order_date'] = pd.to_datetime(df['first_order_date'])
        df['last_order_date'] = pd.to_datetime(df['last_order_date'])
        
        df['days_since_first_order'] = df['first_order_date'].apply(
            lambda x: (today - x.date()).days if pd.notna(x) else None
        )
        df['days_since_last_order'] = df['last_order_date'].apply(
            lambda x: (today - x.date()).days if pd.notna(x) else None
        )
        
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        self.db.truncate_table('customer_metrics', schema='prod')
        rows = self.db.write_dataframe(df, 'customer_metrics', schema='prod', if_exists='append')
        
        logger.info(f"Customer Metrics: {rows} customers")
        return {'table': 'customer_metrics', 'rows': rows}
    
    def build_all(self) -> dict:
        """Build all prod tables"""
        logger.info("="*60)
        logger.info("ETL PRODUCTION LAYER - STARTING")
        logger.info("="*60)
        
        results = {}
        
        results['daily_sales'] = self.build_daily_sales()
        results['monthly_sales'] = self.build_monthly_sales()
        results['daily_category_metrics'] = self.build_daily_category_metrics()
        results['daily_product_metrics'] = self.build_daily_product_metrics()
        results['customer_metrics'] = self.build_customer_metrics()
        
        logger.info("\n" + "="*60)
        logger.info("ETL PRODUCTION LAYER - COMPLETE")
        logger.info("="*60)
        
        total_rows = sum(r['rows'] for r in results.values())
        logger.info(f"Total rows in prod: {total_rows:,}")
        
        return results


def main():
    parser = argparse.ArgumentParser(description='ETL Production Layer')
    parser.add_argument('--table', type=str,
                        help='Build specific table only')
    args = parser.parse_args()
    
    etl = ProdLayerETL()
    
    if args.table:
        method_name = f"build_{args.table}"
        if hasattr(etl, method_name):
            result = getattr(etl, method_name)()
            print(f"\nResult: {result}")
        else:
            print(f"Unknown table: {args.table}")
    else:
        results = etl.build_all()
        print(f"\nResults: {results}")


if __name__ == '__main__':
    main()
