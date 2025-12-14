"""
ETL Pipeline Module
Orchestrates Extract, Transform, Load operations
"""

import pandas as pd
from typing import Optional, Dict, Any, List, Callable
import logging
from datetime import datetime
from pathlib import Path

from db_connector import DatabaseConnector
from data_cleaner import DataCleaner

logger = logging.getLogger(__name__)


class ETLPipeline:
    """ETL Pipeline orchestrator"""
    
    def __init__(
        self,
        pipeline_name: str,
        db_connector: Optional[DatabaseConnector] = None
    ):
        """Initialize ETL pipeline"""
        self.pipeline_name = pipeline_name
        self.db = db_connector or DatabaseConnector()
        self.execution_log = []
        self.start_time = None
        self.end_time = None
        
        logger.info(f"ETL Pipeline '{pipeline_name}' initialized")
    
    def log_step(self, step: str, status: str, details: str, rows: int = 0):
        """Log pipeline step"""
        self.execution_log.append({
            'timestamp': datetime.now(),
            'step': step,
            'status': status,
            'details': details,
            'rows': rows
        })
        logger.info(f"[{step}] {status}: {details}")
    
    def extract_from_database(
        self,
        query: str,
        params: Optional[tuple] = None
    ) -> pd.DataFrame:
        """Extract data from database"""
        self.log_step('EXTRACT', 'START', 'Extracting from database')
        
        try:
            df = self.db.read_sql(query, params)
            self.log_step(
                'EXTRACT',
                'SUCCESS',
                f'Extracted {len(df)} rows',
                len(df)
            )
            return df
        except Exception as e:
            self.log_step('EXTRACT', 'FAILED', str(e))
            raise
    
    def extract_from_csv(
        self,
        file_path: str,
        **kwargs
    ) -> pd.DataFrame:
        """Extract data from CSV file"""
        self.log_step('EXTRACT', 'START', f'Reading CSV: {file_path}')
        
        try:
            df = pd.read_csv(file_path, **kwargs)
            self.log_step(
                'EXTRACT',
                'SUCCESS',
                f'Loaded {len(df)} rows from CSV',
                len(df)
            )
            return df
        except Exception as e:
            self.log_step('EXTRACT', 'FAILED', str(e))
            raise
    
    def transform(
        self,
        df: pd.DataFrame,
        transformations: List[Callable[[pd.DataFrame], pd.DataFrame]]
    ) -> pd.DataFrame:
        """Apply transformation functions"""
        self.log_step('TRANSFORM', 'START', f'Applying {len(transformations)} transformations')
        
        try:
            for i, transform_func in enumerate(transformations, 1):
                df = transform_func(df)
                self.log_step(
                    'TRANSFORM',
                    'PROGRESS',
                    f'Transformation {i}/{len(transformations)} complete',
                    len(df)
                )
            
            self.log_step(
                'TRANSFORM',
                'SUCCESS',
                f'All transformations complete',
                len(df)
            )
            return df
        except Exception as e:
            self.log_step('TRANSFORM', 'FAILED', str(e))
            raise
    
    def load_to_database(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'analytics',
        if_exists: str = 'append'
    ) -> int:
        """Load data to database"""
        self.log_step('LOAD', 'START', f'Loading to {schema}.{table_name}')
        
        try:
            rows_written = self.db.write_dataframe(
                df,
                table_name,
                schema,
                if_exists
            )
            self.log_step(
                'LOAD',
                'SUCCESS',
                f'Loaded {rows_written} rows to database',
                rows_written
            )
            return rows_written
        except Exception as e:
            self.log_step('LOAD', 'FAILED', str(e))
            raise
    
    def load_to_csv(
        self,
        df: pd.DataFrame,
        file_path: str,
        **kwargs
    ) -> int:
        """Load data to CSV file"""
        self.log_step('LOAD', 'START', f'Writing to CSV: {file_path}')
        
        try:
            # Create directory if not exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(file_path, index=False, **kwargs)
            self.log_step(
                'LOAD',
                'SUCCESS',
                f'Written {len(df)} rows to CSV',
                len(df)
            )
            return len(df)
        except Exception as e:
            self.log_step('LOAD', 'FAILED', str(e))
            raise
    
    def run(
        self,
        extract_func: Callable[[], pd.DataFrame],
        transform_funcs: List[Callable[[pd.DataFrame], pd.DataFrame]],
        load_func: Callable[[pd.DataFrame], int]
    ) -> Dict[str, Any]:
        """
        Run complete ETL pipeline
        
        Args:
            extract_func: Function to extract data
            transform_funcs: List of transformation functions
            load_func: Function to load data
            
        Returns:
            Pipeline execution summary
        """
        self.start_time = datetime.now()
        self.log_step('PIPELINE', 'START', f'Starting pipeline: {self.pipeline_name}')
        
        try:
            # Extract
            df = extract_func()
            
            # Transform
            df = self.transform(df, transform_funcs)
            
            # Load
            rows_loaded = load_func(df)
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.log_step(
                'PIPELINE',
                'SUCCESS',
                f'Pipeline completed in {duration:.2f}s',
                rows_loaded
            )
            
            return {
                'status': 'SUCCESS',
                'pipeline_name': self.pipeline_name,
                'start_time': self.start_time,
                'end_time': self.end_time,
                'duration_seconds': duration,
                'rows_processed': rows_loaded
            }
            
        except Exception as e:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.log_step('PIPELINE', 'FAILED', str(e))
            
            return {
                'status': 'FAILED',
                'pipeline_name': self.pipeline_name,
                'start_time': self.start_time,
                'end_time': self.end_time,
                'duration_seconds': duration,
                'error': str(e)
            }
    
    def get_execution_log(self) -> pd.DataFrame:
        """Get pipeline execution log"""
        return pd.DataFrame(self.execution_log)


# Example pipelines
def customer_enrichment_pipeline():
    """Example: Enrich customer data with order statistics"""
    pipeline = ETLPipeline('customer_enrichment')
    
    # Extract
    def extract():
        query = """
        SELECT 
            c.*,
            COUNT(o.order_id) as total_orders,
            SUM(o.total_amount) as total_revenue,
            MAX(o.order_date) as last_order_date
        FROM analytics.customers c
        LEFT JOIN analytics.orders o ON c.customer_id = o.customer_id
        WHERE o.order_status = 'completed'
        GROUP BY c.customer_id
        """
        return pipeline.extract_from_database(query)
    
    # Transform
    def add_customer_segment(df):
        """Add customer segment based on revenue"""
        df['customer_value_segment'] = pd.cut(
            df['total_revenue'],
            bins=[0, 1000, 5000, 10000, float('inf')],
            labels=['Low', 'Medium', 'High', 'VIP']
        )
        return df
    
    def calculate_recency(df):
        """Calculate days since last order"""
        df['days_since_last_order'] = (
            pd.Timestamp.now() - pd.to_datetime(df['last_order_date'])
        ).dt.days
        return df
    
    # Load
    def load(df):
        return pipeline.load_to_database(
            df,
            'customers_enriched',
            if_exists='replace'
        )
    
    # Run pipeline
    result = pipeline.run(
        extract_func=extract,
        transform_funcs=[add_customer_segment, calculate_recency],
        load_func=load
    )
    
    print("\nPipeline Result:")
    print(result)
    
    print("\nExecution Log:")
    print(pipeline.get_execution_log())
    
    return result


if __name__ == "__main__":
    # Run example pipeline
    customer_enrichment_pipeline()