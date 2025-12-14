"""
Data Cleaning Module
Common data cleaning operations for ETL pipelines
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Callable
import logging
import re

logger = logging.getLogger(__name__)


class DataCleaner:
    """Data cleaning utilities"""
    
    def __init__(self, df: pd.DataFrame):
        """Initialize with DataFrame"""
        self.df = df.copy()
        self.original_shape = df.shape
        self.cleaning_log = []
        logger.info(f"DataCleaner initialized with shape {self.original_shape}")
    
    def log_operation(self, operation: str, details: str):
        """Log cleaning operation"""
        self.cleaning_log.append({
            'operation': operation,
            'details': details,
            'shape_after': self.df.shape
        })
        logger.info(f"{operation}: {details}")
    
    def remove_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: str = 'first'
    ) -> 'DataCleaner':
        """Remove duplicate rows"""
        before = len(self.df)
        self.df = self.df.drop_duplicates(subset=subset, keep=keep)
        after = len(self.df)
        removed = before - after
        
        self.log_operation(
            'remove_duplicates',
            f"Removed {removed} duplicates ({removed/before*100:.2f}%)"
        )
        return self
    
    def handle_missing_values(
        self,
        strategy: Dict[str, Any]
    ) -> 'DataCleaner':
        """
        Handle missing values with different strategies
        
        Args:
            strategy: Dict mapping column names to strategies
                     'drop': Drop rows with missing values
                     'fill_value': Fill with specific value
                     'fill_mean': Fill with mean
                     'fill_median': Fill with median
                     'fill_mode': Fill with mode
                     'fill_forward': Forward fill
                     'fill_backward': Backward fill
        """
        for column, method in strategy.items():
            if column not in self.df.columns:
                logger.warning(f"Column {column} not found")
                continue
            
            missing_before = self.df[column].isna().sum()
            
            if method == 'drop':
                self.df = self.df.dropna(subset=[column])
            elif isinstance(method, (int, float, str)):
                self.df[column] = self.df[column].fillna(method)
            elif method == 'fill_mean':
                self.df[column] = self.df[column].fillna(self.df[column].mean())
            elif method == 'fill_median':
                self.df[column] = self.df[column].fillna(self.df[column].median())
            elif method == 'fill_mode':
                self.df[column] = self.df[column].fillna(self.df[column].mode()[0])
            elif method == 'fill_forward':
                self.df[column] = self.df[column].fillna(method='ffill')
            elif method == 'fill_backward':
                self.df[column] = self.df[column].fillna(method='bfill')
            
            missing_after = self.df[column].isna().sum()
            
            self.log_operation(
                'handle_missing',
                f"{column}: {missing_before} → {missing_after} missing values"
            )
        
        return self
    
    def standardize_text(
        self,
        columns: List[str],
        lowercase: bool = True,
        strip: bool = True,
        remove_extra_spaces: bool = True
    ) -> 'DataCleaner':
        """Standardize text columns"""
        for col in columns:
            if col not in self.df.columns:
                continue
            
            if lowercase:
                self.df[col] = self.df[col].str.lower()
            
            if strip:
                self.df[col] = self.df[col].str.strip()
            
            if remove_extra_spaces:
                self.df[col] = self.df[col].str.replace(r'\s+', ' ', regex=True)
            
            self.log_operation(
                'standardize_text',
                f"Standardized {col}"
            )
        
        return self
    
    def remove_outliers(
        self,
        column: str,
        method: str = 'iqr',
        threshold: float = 1.5
    ) -> 'DataCleaner':
        """
        Remove outliers using IQR or Z-score method
        
        Args:
            column: Column name
            method: 'iqr' or 'zscore'
            threshold: IQR multiplier or Z-score threshold
        """
        before = len(self.df)
        
        if method == 'iqr':
            Q1 = self.df[column].quantile(0.25)
            Q3 = self.df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            self.df = self.df[
                (self.df[column] >= lower_bound) & 
                (self.df[column] <= upper_bound)
            ]
        
        elif method == 'zscore':
            z_scores = np.abs(
                (self.df[column] - self.df[column].mean()) / self.df[column].std()
            )
            self.df = self.df[z_scores < threshold]
        
        after = len(self.df)
        removed = before - after
        
        self.log_operation(
            'remove_outliers',
            f"{column}: Removed {removed} outliers ({removed/before*100:.2f}%)"
        )
        
        return self
    
    def convert_datatypes(
        self,
        type_mapping: Dict[str, str]
    ) -> 'DataCleaner':
        """Convert column datatypes"""
        for column, dtype in type_mapping.items():
            if column not in self.df.columns:
                continue
            
            try:
                if dtype == 'datetime':
                    self.df[column] = pd.to_datetime(self.df[column])
                elif dtype == 'category':
                    self.df[column] = self.df[column].astype('category')
                else:
                    self.df[column] = self.df[column].astype(dtype)
                
                self.log_operation(
                    'convert_datatype',
                    f"{column} → {dtype}"
                )
            except Exception as e:
                logger.error(f"Failed to convert {column} to {dtype}: {e}")
        
        return self
    
    def validate_email(
        self,
        column: str,
        remove_invalid: bool = False
    ) -> 'DataCleaner':
        """Validate email addresses"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        valid_mask = self.df[column].str.match(email_pattern, na=False)
        invalid_count = (~valid_mask).sum()
        
        if remove_invalid:
            self.df = self.df[valid_mask]
            self.log_operation(
                'validate_email',
                f"Removed {invalid_count} invalid emails"
            )
        else:
            self.log_operation(
                'validate_email',
                f"Found {invalid_count} invalid emails"
            )
        
        return self
    
    def apply_custom_function(
        self,
        column: str,
        func: Callable,
        new_column: Optional[str] = None
    ) -> 'DataCleaner':
        """Apply custom function to column"""
        target_col = new_column if new_column else column
        self.df[target_col] = self.df[column].apply(func)
        
        self.log_operation(
            'apply_custom_function',
            f"Applied function to {column}"
        )
        
        return self
    
    def get_cleaned_data(self) -> pd.DataFrame:
        """Get cleaned DataFrame"""
        logger.info(f"Cleaning complete: {self.original_shape} → {self.df.shape}")
        return self.df
    
    def get_cleaning_report(self) -> pd.DataFrame:
        """Get cleaning operations report"""
        return pd.DataFrame(self.cleaning_log)


# Convenience functions
def quick_clean(
    df: pd.DataFrame,
    remove_duplicates: bool = True,
    handle_missing: bool = True,
    standardize_text_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """Quick cleaning with common operations"""
    cleaner = DataCleaner(df)
    
    if remove_duplicates:
        cleaner.remove_duplicates()
    
    if handle_missing:
        # Fill numeric with median, categorical with mode
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        strategy = {}
        for col in numeric_cols:
            if df[col].isna().sum() > 0:
                strategy[col] = 'fill_median'
        
        for col in categorical_cols:
            if df[col].isna().sum() > 0:
                strategy[col] = 'fill_mode'
        
        if strategy:
            cleaner.handle_missing_values(strategy)
    
    if standardize_text_cols:
        cleaner.standardize_text(standardize_text_cols)
    
    return cleaner.get_cleaned_data()


# Example usage
if __name__ == "__main__":
    # Create sample data
    df = pd.DataFrame({
        'name': ['  John Doe  ', 'jane smith', 'JOHN DOE', 'Bob Wilson', None],
        'email': ['john@example.com', 'invalid-email', 'jane@test.com', 'bob@company.com', 'test@test.com'],
        'age': [25, 30, 25, np.nan, 35],
        'salary': [50000, 60000, 50000, 75000, 1000000]  # Last one is outlier
    })
    
    print("Original data:")
    print(df)
    print(f"\nShape: {df.shape}")
    
    # Clean data
    cleaner = DataCleaner(df)
    cleaned_df = (
        cleaner
        .remove_duplicates(subset=['name', 'age'])
        .handle_missing_values({
            'name': 'Unknown',
            'age': 'fill_median'
        })
        .standardize_text(['name'])
        .validate_email('email', remove_invalid=True)
        .remove_outliers('salary', method='iqr')
        .get_cleaned_data()
    )
    
    print("\n" + "="*50)
    print("Cleaned data:")
    print(cleaned_df)
    print(f"\nShape: {cleaned_df.shape}")
    
    print("\n" + "="*50)
    print("Cleaning report:")
    print(cleaner.get_cleaning_report())