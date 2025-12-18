"""
Database Connection Module
Handles PostgreSQL connections and query execution using SQLAlchemy

Benefits of SQLAlchemy over raw psycopg2:
- Better connection pooling
- Full pandas compatibility (no warnings)
- Handles edge cases (parameter limits, transactions)
- Cleaner API
"""

import pandas as pd
from typing import Optional, List, Any
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """PostgreSQL Database Connector using SQLAlchemy"""
    
    def __init__(
        self,
        host: str = 'postgres',
        port: int = 5432,
        database: str = 'data_engineer',
        user: str = 'dataengineer',
        password: str = 'dataengineer123'
    ):
        """Initialize database connector with SQLAlchemy engine"""
        self.connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        
        # Create SQLAlchemy engine with connection pooling
        self.engine: Engine = create_engine(
            self.connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True  # Verify connections before use
        )
        
        logger.info(f"Database connector initialized for {database}@{host}")
    
    def execute_query(
        self,
        query: str,
        params: Optional[dict] = None,
        fetch: bool = True
    ) -> Optional[List[tuple]]:
        """
        Execute SQL query using SQLAlchemy
        
        Args:
            query: SQL query string
            params: Query parameters as dict
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            
            if fetch:
                rows = result.fetchall()
                logger.info(f"Query executed, {len(rows)} rows returned")
                return rows
            else:
                conn.commit()
                logger.info("Query executed successfully")
                return None
    
    def read_sql(
        self,
        query: str,
        params: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Execute query and return pandas DataFrame
        
        Args:
            query: SQL query string
            params: Query parameters as dict
            
        Returns:
            pandas DataFrame with query results
        """
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, params=params)
            logger.info(f"Query executed, DataFrame shape: {df.shape}")
            return df
    
    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'analytics',
        if_exists: str = 'append',
        chunksize: int = 500  # Safe batch size for PostgreSQL
    ) -> int:
        """
        Write DataFrame to database table
        
        Args:
            df: pandas DataFrame to write
            table_name: Target table name
            schema: Database schema
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            chunksize: Number of rows per batch
            
        Returns:
            Number of rows written
        """
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize
        )
        logger.info(f"Written {len(df)} rows to {schema}.{table_name}")
        return len(df)
    
    def table_exists(
        self,
        table_name: str,
        schema: str = 'analytics'
    ) -> bool:
        """Check if table exists"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema 
            AND table_name = :table_name
        );
        """
        result = self.execute_query(query, {'schema': schema, 'table_name': table_name})
        return result[0][0] if result else False
    
    def get_table_info(
        self,
        table_name: str,
        schema: str = 'analytics'
    ) -> pd.DataFrame:
        """Get table column information"""
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name = :table_name
        ORDER BY ordinal_position;
        """
        return self.read_sql(query, {'schema': schema, 'table_name': table_name})
    
    def truncate_table(self, table_name: str, schema: str = 'raw') -> None:
        """Truncate a table"""
        query = f"TRUNCATE TABLE {schema}.{table_name} CASCADE"
        self.execute_query(query, fetch=False)
        logger.info(f"Truncated: {schema}.{table_name}")


# Convenience functions
def get_db_connector() -> DatabaseConnector:
    """Get default database connector instance"""
    return DatabaseConnector()


def quick_query(query: str) -> pd.DataFrame:
    """Quick query execution"""
    db = get_db_connector()
    return db.read_sql(query)


# Example usage
if __name__ == "__main__":
    # Test connection
    db = DatabaseConnector()
    
    # Test query
    df = db.read_sql("SELECT COUNT(*) as count FROM analytics.customers")
    print(f"Total customers: {df['count'].iloc[0]}")
    
    # Test table info
    info = db.get_table_info('customers')
    print("\nCustomers table structure:")
    print(info)