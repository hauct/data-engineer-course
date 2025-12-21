#!/usr/bin/env python3
"""
ETL Pipeline Validation Script
Validates data quality across all layers (raw, staging, prod)
"""

import sys
from pathlib import Path
from datetime import datetime

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent))

from db_connector import DatabaseConnector

# Try to import colorama, fallback to no colors if not available
try:
    from colorama import Fore, Style, init
    init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False
    class Fore:
        CYAN = GREEN = RED = YELLOW = ""
    class Style:
        RESET_ALL = ""


class PipelineValidator:
    """Validates ETL pipeline data quality"""
    
    def __init__(self):
        self.db = DatabaseConnector()
        self.issues = []
        self.total_checks = 0
        self.passed_checks = 0
        
    def print_header(self, text):
        """Print section header"""
        print(f"\n{Fore.CYAN}{'='*70}")
        print(f"{text}")
        print(f"{'='*70}{Style.RESET_ALL}")
    
    def print_check(self, name, passed, details=""):
        """Print check result"""
        self.total_checks += 1
        if passed:
            self.passed_checks += 1
            status = f"{Fore.GREEN}✅ PASS"
        else:
            status = f"{Fore.RED}❌ FAIL"
            
        print(f"  {name:.<50} {status}{Style.RESET_ALL}")
        
        if details and not passed:
            print(f"    {Fore.YELLOW}→ {details}{Style.RESET_ALL}")
            self.issues.append((name, details))
    
    def validate_schemas(self):
        """Check if all required schemas exist"""
        self.print_header("1. SCHEMA VALIDATION")
        
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name IN ('raw', 'staging', 'prod')
        ORDER BY schema_name
        """
        
        try:
            schemas = self.db.read_sql(query)['schema_name'].tolist()
            
            for schema in ['raw', 'staging', 'prod']:
                self.print_check(
                    f"Schema '{schema}' exists",
                    schema in schemas,
                    f"Schema not found. Run: make etl-setup-schema"
                )
        except Exception as e:
            self.print_check("Schema validation", False, str(e))
    
    def validate_tables(self):
        """Check if all required tables exist"""
        self.print_header("2. TABLE VALIDATION")
        
        expected_tables = {
            'raw': ['customers', 'products', 'orders', 'order_items'],
            'staging': ['customers', 'products', 'orders', 'order_items'],
            'prod': ['daily_sales', 'monthly_sales', 'customer_metrics',
                    'daily_category_metrics', 'daily_product_metrics']
        }
        
        for schema, tables in expected_tables.items():
            for table in tables:
                query = f"""
                SELECT COUNT(*) as count
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                """
                
                try:
                    exists = self.db.read_sql(query)['count'][0] > 0
                    self.print_check(
                        f"Table '{schema}.{table}' exists",
                        exists,
                        f"Table not found. Run: make etl-setup-schema"
                    )
                except Exception as e:
                    self.print_check(f"Table '{schema}.{table}'", False, str(e))
    
    def validate_data_flow(self):
        """Check data flow through pipeline"""
        self.print_header("3. DATA FLOW VALIDATION")
        
        # Check row counts
        queries = {
            'raw.customers': "SELECT COUNT(*) as count FROM raw.customers",
            'staging.customers': "SELECT COUNT(*) as count FROM staging.customers",
            'prod.customer_metrics': "SELECT COUNT(*) as count FROM prod.customer_metrics",
            'raw.orders': "SELECT COUNT(*) as count FROM raw.orders",
            'staging.orders': "SELECT COUNT(*) as count FROM staging.orders",
            'prod.daily_sales': "SELECT COUNT(*) as count FROM prod.daily_sales"
        }
        
        counts = {}
        for name, query in queries.items():
            try:
                counts[name] = self.db.read_sql(query)['count'][0]
            except:
                counts[name] = 0
        
        # Validate RAW layer
        self.print_check(
            "Raw layer has customer data",
            counts['raw.customers'] > 0,
            f"Found {counts['raw.customers']} rows. Run: make etl-generate-raw-test"
        )
        
        self.print_check(
            "Raw layer has order data",
            counts['raw.orders'] > 0,
            f"Found {counts['raw.orders']} rows. Run: make etl-generate-raw-test"
        )
        
        # Validate STAGING layer
        self.print_check(
            "Staging layer has customer data",
            counts['staging.customers'] > 0,
            f"Found {counts['staging.customers']} rows. Run: make etl-run-stg"
        )
        
        self.print_check(
            "Staging layer has order data",
            counts['staging.orders'] > 0,
            f"Found {counts['staging.orders']} rows. Run: make etl-run-stg"
        )
        
        # Validate PRODUCTION layer
        self.print_check(
            "Production layer has customer metrics",
            counts['prod.customer_metrics'] > 0,
            f"Found {counts['prod.customer_metrics']} rows. Run: make etl-run-prod"
        )
        
        self.print_check(
            "Production layer has daily sales",
            counts['prod.daily_sales'] > 0,
            f"Found {counts['prod.daily_sales']} rows. Run: make etl-run-prod"
        )
        
        # Check data loss percentage
        if counts['raw.customers'] > 0 and counts['staging.customers'] > 0:
            loss_pct = (counts['raw.customers'] - counts['staging.customers']) / counts['raw.customers'] * 100
            self.print_check(
                "Data loss < 20% (raw → staging)",
                loss_pct < 20,
                f"Loss: {loss_pct:.1f}%. Check data quality issues."
            )
    
    def validate_data_quality(self):
        """Check data quality in staging layer"""
        self.print_header("4. DATA QUALITY VALIDATION")
        
        quality_checks = [
            ("No duplicate customer_ids", 
             "SELECT COUNT(*) - COUNT(DISTINCT customer_id) as dups FROM staging.customers",
             0),
            
            ("No duplicate emails",
             "SELECT COUNT(*) - COUNT(DISTINCT email) as dups FROM staging.customers",
             0),
            
            ("No NULL emails",
             "SELECT COUNT(*) as nulls FROM staging.customers WHERE email IS NULL",
             0),
            
            ("No invalid email formats",
             "SELECT COUNT(*) as invalid FROM staging.customers WHERE email NOT LIKE '%@%.%'",
             0),
            
            ("No negative prices",
             "SELECT COUNT(*) as invalid FROM staging.products WHERE price < 0",
             0),
            
            ("No zero prices",
             "SELECT COUNT(*) as invalid FROM staging.products WHERE price = 0",
             0),
            
            ("No orphaned orders (missing customer)",
             """SELECT COUNT(*) as orphans FROM staging.orders o 
                WHERE NOT EXISTS (SELECT 1 FROM staging.customers c WHERE c.customer_id = o.customer_id)""",
             0),
            
            ("No orphaned order_items (missing order)",
             """SELECT COUNT(*) as orphans FROM staging.order_items oi 
                WHERE NOT EXISTS (SELECT 1 FROM staging.orders o WHERE o.order_id = oi.order_id)""",
             0),
            
            ("No orphaned order_items (missing product)",
             """SELECT COUNT(*) as orphans FROM staging.order_items oi 
                WHERE NOT EXISTS (SELECT 1 FROM staging.products p WHERE p.product_id = oi.product_id)""",
             0),
            
            ("No future order dates",
             "SELECT COUNT(*) as invalid FROM staging.orders WHERE order_date > CURRENT_DATE",
             0),
            
            ("No invalid discount percentages",
             "SELECT COUNT(*) as invalid FROM staging.order_items WHERE discount_percent < 0 OR discount_percent > 100",
             0),
        ]
        
        for name, query, expected in quality_checks:
            try:
                result = self.db.read_sql(query).iloc[0, 0]
                self.print_check(
                    name,
                    result == expected,
                    f"Found {result} issues"
                )
            except Exception as e:
                self.print_check(name, False, str(e))
    
    def validate_business_rules(self):
        """Check business rules and data consistency"""
        self.print_header("5. BUSINESS RULES VALIDATION")
        
        # Revenue consistency between staging and prod
        revenue_query = """
        WITH staging_rev AS (
            SELECT COALESCE(SUM(total_amount), 0) as total 
            FROM staging.orders 
            WHERE order_status = 'completed'
        ),
        prod_rev AS (
            SELECT COALESCE(SUM(total_revenue), 0) as total 
            FROM prod.daily_sales
        )
        SELECT ABS(s.total - p.total) as diff
        FROM staging_rev s, prod_rev p
        """
        
        try:
            diff = self.db.read_sql(revenue_query)['diff'][0]
            self.print_check(
                "Revenue matches (staging vs prod)",
                diff < 0.01,
                f"Difference: ${diff:.2f}"
            )
        except Exception as e:
            self.print_check("Revenue consistency", False, str(e))
        
        # Order amount matches sum of items
        order_amount_query = """
        SELECT COUNT(*) as mismatches FROM (
            SELECT 
                o.order_id,
                o.total_amount as order_total,
                COALESCE(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)), 0) as items_total
            FROM staging.orders o
            LEFT JOIN staging.order_items oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.total_amount
            HAVING ABS(o.total_amount - COALESCE(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)), 0)) > 0.01
        ) mismatches
        """
        
        try:
            mismatches = self.db.read_sql(order_amount_query)['mismatches'][0]
            self.print_check(
                "Order totals match item sums",
                mismatches == 0,
                f"Found {mismatches} mismatches"
            )
        except Exception as e:
            self.print_check("Order amount validation", False, str(e))
        
        # Customer segment values
        segment_query = """
        SELECT COUNT(*) as invalid 
        FROM staging.customers 
        WHERE customer_segment NOT IN ('Premium', 'Standard', 'Basic')
        """
        
        try:
            invalid = self.db.read_sql(segment_query)['invalid'][0]
            self.print_check(
                "Valid customer segments only",
                invalid == 0,
                f"Found {invalid} invalid segments"
            )
        except Exception as e:
            self.print_check("Customer segment validation", False, str(e))
        
        # Order status values
        status_query = """
        SELECT COUNT(*) as invalid 
        FROM staging.orders 
        WHERE order_status NOT IN ('pending', 'completed', 'cancelled')
        """
        
        try:
            invalid = self.db.read_sql(status_query)['invalid'][0]
            self.print_check(
                "Valid order statuses only",
                invalid == 0,
                f"Found {invalid} invalid statuses"
            )
        except Exception as e:
            self.print_check("Order status validation", False, str(e))
    
    def validate_metadata(self):
        """Check metadata and timestamps"""
        self.print_header("6. METADATA VALIDATION")
        
        # Check raw layer metadata
        metadata_query = """
        SELECT 
            COUNT(DISTINCT _partition_date) as partitions,
            MIN(_ingested_at) as first_ingestion,
            MAX(_ingested_at) as last_ingestion
        FROM raw.customers
        """
        
        try:
            metadata = self.db.read_sql(metadata_query)
            partitions = metadata['partitions'][0]
            
            self.print_check(
                "Raw layer has partition metadata",
                partitions > 0,
                f"Found {partitions} partitions"
            )
            
            if partitions > 0:
                first = metadata['first_ingestion'][0]
                last = metadata['last_ingestion'][0]
                print(f"    {Fore.CYAN}→ First ingestion: {first}{Style.RESET_ALL}")
                print(f"    {Fore.CYAN}→ Last ingestion: {last}{Style.RESET_ALL}")
        except Exception as e:
            self.print_check("Metadata validation", False, str(e))
        
        # Check staging timestamps
        timestamp_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(*) - COUNT(created_at) as missing_created,
            COUNT(*) - COUNT(updated_at) as missing_updated
        FROM staging.customers
        """
        
        try:
            timestamps = self.db.read_sql(timestamp_query)
            missing_created = timestamps['missing_created'][0]
            missing_updated = timestamps['missing_updated'][0]
            
            self.print_check(
                "Staging has created_at timestamps",
                missing_created == 0,
                f"Missing {missing_created} timestamps"
            )
            
            self.print_check(
                "Staging has updated_at timestamps",
                missing_updated == 0,
                f"Missing {missing_updated} timestamps"
            )
        except Exception as e:
            self.print_check("Timestamp validation", False, str(e))
    
    def print_summary(self):
        """Print validation summary"""
        self.print_header("VALIDATION SUMMARY")
        
        # Calculate pass rate
        pass_rate = (self.passed_checks / self.total_checks * 100) if self.total_checks > 0 else 0
        
        print(f"\n  Total Checks: {self.total_checks}")
        print(f"  Passed: {Fore.GREEN}{self.passed_checks}{Style.RESET_ALL}")
        print(f"  Failed: {Fore.RED}{self.total_checks - self.passed_checks}{Style.RESET_ALL}")
        print(f"  Pass Rate: {pass_rate:.1f}%")
        
        # Print issues if any
        if len(self.issues) > 0:
            print(f"\n{Fore.RED}❌ FOUND {len(self.issues)} ISSUE(S):{Style.RESET_ALL}")
            for i, (name, details) in enumerate(self.issues, 1):
                print(f"  {i}. {name}")
                print(f"     {Fore.YELLOW}→ {details}{Style.RESET_ALL}")
        else:
            print(f"\n{Fore.GREEN}✅ ALL CHECKS PASSED!{Style.RESET_ALL}")
        
        print(f"\n{'='*70}")
        
        # Overall status
        if len(self.issues) == 0:
            print(f"{Fore.GREEN}🎉 PIPELINE VALIDATION SUCCESSFUL!{Style.RESET_ALL}")
            return 0
        else:
            print(f"{Fore.RED}❌ PIPELINE VALIDATION FAILED!{Style.RESET_ALL}")
            print(f"\n{Fore.YELLOW}Recommendations:{Style.RESET_ALL}")
            print("  1. Review failed checks above")
            print("  2. Check ETL logs: make etl-logs")
            print("  3. Debug layers: make etl-debug-raw, etl-debug-stg, etl-debug-prod")
            print("  4. Re-run pipeline: make etl-run-all")
            return 1
    
    def run_all(self):
        """Run all validations"""
        print(f"\n{Fore.CYAN}{'='*70}")
        print(f"ETL PIPELINE VALIDATION")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}{Style.RESET_ALL}")
        
        try:
            self.validate_schemas()
            self.validate_tables()
            self.validate_data_flow()
            self.validate_data_quality()
            self.validate_business_rules()
            self.validate_metadata()
        except Exception as e:
            print(f"\n{Fore.RED}❌ Validation error: {e}{Style.RESET_ALL}")
            return 1
        
        return self.print_summary()


def main():
    """Main entry point"""
    try:
        validator = PipelineValidator()
        exit_code = validator.run_all()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Validation interrupted by user{Style.RESET_ALL}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Fore.RED}Fatal error: {e}{Style.RESET_ALL}")
        sys.exit(1)


if __name__ == "__main__":
    main()