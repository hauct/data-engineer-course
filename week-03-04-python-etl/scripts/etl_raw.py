"""
ETL Raw Layer - Ingest from raw_data folder to PostgreSQL raw schema

Features:
- Reads Parquet files from raw_data folder
- Loads to raw schema without transformation
- Adds metadata columns: _ingested_at, _source_file, _partition_date
- Immutable (append-only)
"""

import pandas as pd
from pathlib import Path
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


class RawLayerETL:
    """ETL for ingesting raw data into PostgreSQL raw schema"""
    
    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        self.db = db_connector or DatabaseConnector()
        self.raw_data_dir = Path(__file__).parent.parent / 'raw_data'
        
    def get_partition_dates(self, table_name: str) -> list:
        """Get all partition dates for a table"""
        table_dir = self.raw_data_dir / table_name
        if not table_dir.exists():
            return []
        
        partitions = sorted([
            d.name for d in table_dir.iterdir() 
            if d.is_dir() and (d / 'data.parquet').exists()
        ])
        return partitions
    
    def get_ingested_dates(self, table_name: str) -> set:
        """Get dates already ingested to raw schema"""
        try:
            query = f"SELECT DISTINCT _partition_date FROM raw.{table_name}"
            df = self.db.read_sql(query)
            return set(df['_partition_date'].astype(str).tolist())
        except Exception:
            return set()
    
    def ingest_partition(
        self,
        table_name: str,
        partition_date: str,
        force: bool = False
    ) -> int:
        """Ingest a single partition to raw schema"""
        
        parquet_path = self.raw_data_dir / table_name / partition_date / 'data.parquet'
        
        if not parquet_path.exists():
            logger.warning(f"Partition not found: {parquet_path}")
            return 0
        
        # Read parquet
        df = pd.read_parquet(parquet_path)
        
        if df.empty:
            logger.info(f"Empty partition: {table_name}/{partition_date}")
            return 0
        
        # Add metadata columns
        df['_ingested_at'] = datetime.now()
        df['_source_file'] = str(parquet_path)
        df['_partition_date'] = partition_date
        
        # Load to raw schema (append mode)
        rows = self.db.write_dataframe(
            df,
            table_name=table_name,
            schema='raw',
            if_exists='append'
        )
        
        logger.info(f"Ingested {rows} rows: raw.{table_name} [{partition_date}]")
        return rows
    
    def ingest_table(
        self,
        table_name: str,
        incremental: bool = True
    ) -> dict:
        """Ingest all partitions for a table"""
        
        all_dates = self.get_partition_dates(table_name)
        
        if incremental:
            ingested_dates = self.get_ingested_dates(table_name)
            dates_to_process = [d for d in all_dates if d not in ingested_dates]
            logger.info(f"Incremental mode: {len(dates_to_process)} new partitions to ingest")
        else:
            dates_to_process = all_dates
            logger.info(f"Full mode: {len(dates_to_process)} partitions to ingest")
        
        total_rows = 0
        for partition_date in dates_to_process:
            rows = self.ingest_partition(table_name, partition_date)
            total_rows += rows
        
        return {
            'table': table_name,
            'partitions_processed': len(dates_to_process),
            'total_rows': total_rows
        }
    
    def ingest_all(self, incremental: bool = True) -> dict:
        """Ingest all tables"""
        tables = ['customers', 'products', 'orders', 'order_items']
        results = {}
        
        logger.info("="*60)
        logger.info("ETL RAW LAYER - STARTING")
        logger.info("="*60)
        
        for table in tables:
            logger.info(f"\nProcessing: {table}")
            result = self.ingest_table(table, incremental)
            results[table] = result
        
        logger.info("\n" + "="*60)
        logger.info("ETL RAW LAYER - COMPLETE")
        logger.info("="*60)
        
        total_rows = sum(r['total_rows'] for r in results.values())
        total_partitions = sum(r['partitions_processed'] for r in results.values())
        
        logger.info(f"Total partitions: {total_partitions}")
        logger.info(f"Total rows ingested: {total_rows:,}")
        
        return results
    
    def truncate_all(self):
        """Truncate all raw tables (for full refresh)"""
        tables = ['order_items', 'orders', 'products', 'customers']
        
        logger.info("Truncating raw tables...")
        for table in tables:
            self.db.truncate_table(table, schema='raw')


def main():
    parser = argparse.ArgumentParser(description='ETL Raw Layer')
    parser.add_argument('--full', action='store_true',
                        help='Full refresh (truncate and reload)')
    parser.add_argument('--table', type=str,
                        help='Process specific table only')
    args = parser.parse_args()
    
    etl = RawLayerETL()
    
    if args.full:
        etl.truncate_all()
    
    if args.table:
        result = etl.ingest_table(args.table, incremental=not args.full)
        print(f"\nResult: {result}")
    else:
        results = etl.ingest_all(incremental=not args.full)
        print(f"\nResults: {results}")


if __name__ == '__main__':
    main()
