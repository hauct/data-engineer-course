"""
Data Validation Module
Comprehensive data quality checks and validation rules
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Callable
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class ValidationResult:
    """Validation result container"""
    
    def __init__(self, rule_name: str, passed: bool, message: str, failed_rows: int = 0):
        self.rule_name = rule_name
        self.passed = passed
        self.message = message
        self.failed_rows = failed_rows
        self.timestamp = datetime.now()
    
    def __repr__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        return f"{status} | {self.rule_name}: {self.message}"


class DataValidator:
    """Data validation framework"""
    
    def __init__(self, df: pd.DataFrame, dataset_name: str = "dataset"):
        """Initialize validator with DataFrame"""
        self.df = df
        self.dataset_name = dataset_name
        self.results: List[ValidationResult] = []
        logger.info(f"DataValidator initialized for '{dataset_name}' with shape {df.shape}")
    
    def add_result(self, result: ValidationResult):
        """Add validation result"""
        self.results.append(result)
        logger.info(str(result))
    
    # ============= COMPLETENESS CHECKS =============
    
    def check_no_nulls(self, columns: List[str]) -> 'DataValidator':
        """Check that specified columns have no null values"""
        for col in columns:
            if col not in self.df.columns:
                self.add_result(ValidationResult(
                    f"no_nulls_{col}",
                    False,
                    f"Column '{col}' not found in dataset"
                ))
                continue
            
            null_count = self.df[col].isna().sum()
            passed = null_count == 0
            
            self.add_result(ValidationResult(
                f"no_nulls_{col}",
                passed,
                f"Found {null_count} null values in '{col}'" if not passed else f"No nulls in '{col}'",
                null_count
            ))
        
        return self
    
    def check_completeness_threshold(
        self,
        columns: List[str],
        threshold: float = 0.95
    ) -> 'DataValidator':
        """Check that columns meet completeness threshold (% non-null)"""
        for col in columns:
            if col not in self.df.columns:
                continue
            
            completeness = self.df[col].notna().sum() / len(self.df)
            passed = completeness >= threshold
            
            self.add_result(ValidationResult(
                f"completeness_{col}",
                passed,
                f"Completeness: {completeness:.2%} (threshold: {threshold:.2%})",
                int(len(self.df) * (1 - completeness))
            ))
        
        return self
    
    # ============= UNIQUENESS CHECKS =============
    
    def check_unique(self, columns: List[str]) -> 'DataValidator':
        """Check that columns have unique values"""
        for col in columns:
            if col not in self.df.columns:
                continue
            
            duplicate_count = self.df[col].duplicated().sum()
            passed = duplicate_count == 0
            
            self.add_result(ValidationResult(
                f"unique_{col}",
                passed,
                f"Found {duplicate_count} duplicates in '{col}'" if not passed else f"All values unique in '{col}'",
                duplicate_count
            ))
        
        return self
    
    def check_primary_key(self, columns: List[str]) -> 'DataValidator':
        """Check that combination of columns forms a valid primary key"""
        duplicate_count = self.df.duplicated(subset=columns).sum()
        passed = duplicate_count == 0
        
        key_name = "+".join(columns)
        self.add_result(ValidationResult(
            f"primary_key_{key_name}",
            passed,
            f"Found {duplicate_count} duplicate keys" if not passed else "Primary key is valid",
            duplicate_count
        ))
        
        return self
    
    # ============= VALIDITY CHECKS =============
    
    def check_data_type(
        self,
        column: str,
        expected_type: type
    ) -> 'DataValidator':
        """Check column data type"""
        if column not in self.df.columns:
            return self
        
        actual_type = self.df[column].dtype
        
        # Map pandas dtypes to Python types
        type_mapping = {
            int: ['int64', 'int32', 'int16', 'int8'],
            float: ['float64', 'float32'],
            str: ['object'],
            bool: ['bool']
        }
        
        expected_dtypes = type_mapping.get(expected_type, [])
        passed = str(actual_type) in expected_dtypes
        
        self.add_result(ValidationResult(
            f"data_type_{column}",
            passed,
            f"Expected {expected_type.__name__}, got {actual_type}"
        ))
        
        return self
    
    def check_value_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> 'DataValidator':
        """Check that numeric values are within range"""
        if column not in self.df.columns:
            return self
        
        violations = 0
        
        if min_value is not None:
            violations += (self.df[column] < min_value).sum()
        
        if max_value is not None:
            violations += (self.df[column] > max_value).sum()
        
        passed = violations == 0
        
        range_str = f"[{min_value}, {max_value}]"
        self.add_result(ValidationResult(
            f"value_range_{column}",
            passed,
            f"Found {violations} values outside range {range_str}" if not passed else f"All values in range {range_str}",
            violations
        ))
        
        return self
    
    def check_allowed_values(
        self,
        column: str,
        allowed_values: List[Any]
    ) -> 'DataValidator':
        """Check that column values are in allowed list"""
        if column not in self.df.columns:
            return self
        
        invalid_mask = ~self.df[column].isin(allowed_values)
        invalid_count = invalid_mask.sum()
        passed = invalid_count == 0
        
        if not passed:
            invalid_values = self.df.loc[invalid_mask, column].unique()[:5]
            invalid_str = ", ".join(map(str, invalid_values))
            message = f"Found {invalid_count} invalid values (e.g., {invalid_str})"
        else:
            message = f"All values in allowed list"
        
        self.add_result(ValidationResult(
            f"allowed_values_{column}",
            passed,
            message,
            invalid_count
        ))
        
        return self
    
    def check_regex_pattern(
        self,
        column: str,
        pattern: str,
        pattern_name: str = "pattern"
    ) -> 'DataValidator':
        """Check that string values match regex pattern"""
        if column not in self.df.columns:
            return self
        
        valid_mask = self.df[column].astype(str).str.match(pattern, na=False)
        invalid_count = (~valid_mask).sum()
        passed = invalid_count == 0
        
        self.add_result(ValidationResult(
            f"regex_{column}_{pattern_name}",
            passed,
            f"Found {invalid_count} values not matching {pattern_name}" if not passed else f"All values match {pattern_name}",
            invalid_count
        ))
        
        return self
    
    def check_email_format(self, column: str) -> 'DataValidator':
        """Check email format"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return self.check_regex_pattern(column, email_pattern, "email_format")
    
    def check_phone_format(self, column: str) -> 'DataValidator':
        """Check phone format (basic)"""
        phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
        return self.check_regex_pattern(column, phone_pattern, "phone_format")
    
    # ============= CONSISTENCY CHECKS =============
    
    def check_referential_integrity(
        self,
        foreign_key_col: str,
        reference_df: pd.DataFrame,
        reference_key_col: str
    ) -> 'DataValidator':
        """Check foreign key referential integrity"""
        if foreign_key_col not in self.df.columns:
            return self
        
        orphaned_mask = ~self.df[foreign_key_col].isin(reference_df[reference_key_col])
        orphaned_count = orphaned_mask.sum()
        passed = orphaned_count == 0
        
        self.add_result(ValidationResult(
            f"referential_integrity_{foreign_key_col}",
            passed,
            f"Found {orphaned_count} orphaned foreign keys" if not passed else "All foreign keys valid",
            orphaned_count
        ))
        
        return self
    
    def check_date_order(
        self,
        start_date_col: str,
        end_date_col: str
    ) -> 'DataValidator':
        """Check that start date is before end date"""
        if start_date_col not in self.df.columns or end_date_col not in self.df.columns:
            return self
        
        start_dates = pd.to_datetime(self.df[start_date_col])
        end_dates = pd.to_datetime(self.df[end_date_col])
        
        violations = (start_dates > end_dates).sum()
        passed = violations == 0
        
        self.add_result(ValidationResult(
            f"date_order_{start_date_col}_{end_date_col}",
            passed,
            f"Found {violations} cases where start > end" if not passed else "Date order is valid",
            violations
        ))
        
        return self
    
    def check_logical_consistency(
        self,
        condition: Callable[[pd.DataFrame], pd.Series],
        rule_name: str,
        rule_description: str
    ) -> 'DataValidator':
        """Check custom logical consistency rule"""
        try:
            violations_mask = ~condition(self.df)
            violations = violations_mask.sum()
            passed = violations == 0
            
            self.add_result(ValidationResult(
                f"logical_{rule_name}",
                passed,
                f"Found {violations} violations of '{rule_description}'" if not passed else f"Rule '{rule_description}' satisfied",
                violations
            ))
        except Exception as e:
            self.add_result(ValidationResult(
                f"logical_{rule_name}",
                False,
                f"Error checking rule: {str(e)}"
            ))
        
        return self
    
    # ============= STATISTICAL CHECKS =============
    
    def check_outliers(
        self,
        column: str,
        method: str = 'iqr',
        threshold: float = 3.0
    ) -> 'DataValidator':
        """Check for statistical outliers"""
        if column not in self.df.columns:
            return self
        
        if method == 'iqr':
            Q1 = self.df[column].quantile(0.25)
            Q3 = self.df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = ((self.df[column] < lower_bound) | (self.df[column] > upper_bound)).sum()
        
        elif method == 'zscore':
            z_scores = np.abs((self.df[column] - self.df[column].mean()) / self.df[column].std())
            outliers = (z_scores > threshold).sum()
        
        else:
            outliers = 0
        
        # Outliers are informational, not necessarily failures
        self.add_result(ValidationResult(
            f"outliers_{column}",
            True,
            f"Found {outliers} potential outliers ({outliers/len(self.df)*100:.2f}%)",
            outliers
        ))
        
        return self
    
    def check_distribution(
        self,
        column: str,
        expected_mean: Optional[float] = None,
        expected_std: Optional[float] = None,
        tolerance: float = 0.1
    ) -> 'DataValidator':
        """Check statistical distribution properties"""
        if column not in self.df.columns:
            return self
        
        actual_mean = self.df[column].mean()
        actual_std = self.df[column].std()
        
        passed = True
        messages = []
        
        if expected_mean is not None:
            mean_diff = abs(actual_mean - expected_mean) / expected_mean
            if mean_diff > tolerance:
                passed = False
                messages.append(f"Mean deviation: {mean_diff:.2%}")
        
        if expected_std is not None:
            std_diff = abs(actual_std - expected_std) / expected_std
            if std_diff > tolerance:
                passed = False
                messages.append(f"Std deviation: {std_diff:.2%}")
        
        message = ", ".join(messages) if messages else "Distribution within tolerance"
        
        self.add_result(ValidationResult(
            f"distribution_{column}",
            passed,
            message
        ))
        
        return self
    
    # ============= REPORTING =============
    
    def get_summary(self) -> Dict[str, Any]:
        """Get validation summary"""
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        failed_rules = total_rules - passed_rules
        total_failed_rows = sum(r.failed_rows for r in self.results)
        
        return {
            'dataset_name': self.dataset_name,
            'total_rows': len(self.df),
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'failed_rules': failed_rules,
            'pass_rate': passed_rules / total_rules if total_rules > 0 else 0,
            'total_failed_rows': total_failed_rows
        }
    
    def get_report(self) -> pd.DataFrame:
        """Get detailed validation report"""
        report_data = []
        for result in self.results:
            report_data.append({
                'rule_name': result.rule_name,
                'status': 'PASS' if result.passed else 'FAIL',
                'message': result.message,
                'failed_rows': result.failed_rows,
                'timestamp': result.timestamp
            })
        
        return pd.DataFrame(report_data)
    
    def print_summary(self):
        """Print validation summary"""
        summary = self.get_summary()
        
        print("\n" + "="*60)
        print(f"📊 VALIDATION SUMMARY: {summary['dataset_name']}")
        print("="*60)
        print(f"Total Rows: {summary['total_rows']:,}")
        print(f"Total Rules: {summary['total_rules']}")
        print(f"✅ Passed: {summary['passed_rules']}")
        print(f"❌ Failed: {summary['failed_rules']}")
        print(f"Pass Rate: {summary['pass_rate']:.1%}")
        print(f"Total Failed Rows: {summary['total_failed_rows']:,}")
        print("="*60)
        
        if summary['failed_rules'] > 0:
            print("\n❌ FAILED RULES:")
            for result in self.results:
                if not result.passed:
                    print(f"  • {result}")
        
        print()
    
    def assert_valid(self):
        """Raise exception if any validation failed"""
        failed = [r for r in self.results if not r.passed]
        if failed:
            error_msg = f"Validation failed: {len(failed)} rule(s) failed\n"
            error_msg += "\n".join(f"  • {r}" for r in failed)
            raise ValueError(error_msg)


# Convenience function
def validate_dataframe(
    df: pd.DataFrame,
    rules: Dict[str, Any],
    dataset_name: str = "dataset"
) -> DataValidator:
    """
    Quick validation with rule dictionary
    
    Example rules:
    {
        'no_nulls': ['customer_id', 'email'],
        'unique': ['customer_id'],
        'email_format': ['email'],
        'value_range': {'age': {'min': 0, 'max': 120}}
    }
    """
    validator = DataValidator(df, dataset_name)
    
    if 'no_nulls' in rules:
        validator.check_no_nulls(rules['no_nulls'])
    
    if 'unique' in rules:
        validator.check_unique(rules['unique'])
    
    if 'email_format' in rules:
        for col in rules['email_format']:
            validator.check_email_format(col)
    
    if 'value_range' in rules:
        for col, range_def in rules['value_range'].items():
            validator.check_value_range(
                col,
                range_def.get('min'),
                range_def.get('max')
            )
    
    if 'allowed_values' in rules:
        for col, values in rules['allowed_values'].items():
            validator.check_allowed_values(col, values)
    
    return validator


# Example usage
if __name__ == "__main__":
    # Create sample data with issues
    df = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5, 5],  # Duplicate
        'name': ['John', 'Jane', None, 'Bob', 'Alice', 'Charlie'],  # Null
        'email': ['john@test.com', 'invalid-email', 'jane@test.com', 'bob@test.com', 'alice@test.com', 'charlie@test.com'],  # Invalid email
        'age': [25, 30, 35, -5, 200, 40],  # Out of range
        'status': ['active', 'active', 'inactive', 'invalid', 'active', 'active'],  # Invalid value
        'signup_date': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01']),
        'last_login': pd.to_datetime(['2024-01-15', '2024-02-15', '2024-03-15', '2024-03-01', '2024-05-15', '2024-06-15'])
    })
    
    print("Sample Data:")
    print(df)
    
    # Run validations
    validator = (
        DataValidator(df, "customers")
        .check_no_nulls(['customer_id', 'name', 'email'])
        .check_unique(['customer_id'])
        .check_primary_key(['customer_id'])
        .check_email_format('email')
        .check_value_range('age', min_value=0, max_value=120)
        .check_allowed_values('status', ['active', 'inactive', 'suspended'])
        .check_date_order('signup_date', 'last_login')
        .check_outliers('age')
    )
    
    # Print summary
    validator.print_summary()
    
    # Get detailed report
    print("\nDetailed Report:")
    print(validator.get_report())