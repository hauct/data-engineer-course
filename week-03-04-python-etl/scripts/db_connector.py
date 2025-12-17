"""
Database Connection Module
Handles PostgreSQL connections and query execution
"""

import psycopg2
import pandas as pd
from typing import Optional, Dict, Any, List
import logging
from contextlib import contextmanager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """PostgreSQL Database Connector with connection pooling"""
    
    def __init__(
        self,
        host: str = 'postgres',
        port: int = 5432,
        database: str = 'data_engineer',
        user: str = 'dataengineer',
        password: str = 'dataengineer123'
    ):
        """Initialize database connector"""
        self.config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        logger.info(f"Database connector initialized for {database}@{host}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            logger.debug("Database connection established")
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed")
    
    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = True
    ) -> Optional[List[tuple]]:
        """
        Execute SQL query
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                
                if fetch:
                    results = cursor.fetchall()
                    logger.info(f"Query executed, {len(results)} rows returned")
                    return results
                else:
                    logger.info("Query executed successfully")
                    return None
    
    def read_sql(
        self,
        query: str,
        params: Optional[tuple] = None
    ) -> pd.DataFrame:
        """
        Execute query and return pandas DataFrame
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            pandas DataFrame with query results
        """
        with self.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            logger.info(f"Query executed, DataFrame shape: {df.shape}")
            return df
    
    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'analytics',
        if_exists: str = 'append',
        chunksize: int = 1000
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
        with self.get_connection() as conn:
            rows_written = df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method='multi'
            )
            logger.info(f"Written {len(df)} rows to {schema}.{table_name}")
            return len(df)
    
    def execute_many(
        self,
        query: str,
        data: List[tuple]
    ) -> int:
        """
        Execute query with multiple parameter sets
        
        Args:
            query: SQL query with placeholders
            data: List of parameter tuples
            
        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, data)
                rows_affected = cursor.rowcount
                logger.info(f"Batch insert: {rows_affected} rows affected")
                return rows_affected
    
    def table_exists(
        self,
        table_name: str,
        schema: str = 'analytics'
    ) -> bool:
        """Check if table exists"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        );
        """
        result = self.execute_query(query, (schema, table_name))
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
        WHERE table_schema = %s
        AND table_name = %s
        ORDER BY ordinal_position;
        """
        return self.read_sql(query, (schema, table_name))


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