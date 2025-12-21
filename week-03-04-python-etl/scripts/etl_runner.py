#!/usr/bin/env python3
"""
ETL Pipeline Runner with Progress Tracking and Error Handling
Orchestrates the complete ETL pipeline execution
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent))

from db_connector import DatabaseConnector
from etl_raw import RawLayerETL
from etl_stg import StagingLayerETL
from etl_prod import ProdLayerETL

# Try to import colorama, fallback to no colors if not available
try:
    from colorama import Fore, Style, init
    init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False
    class Fore:
        CYAN = GREEN = RED = YELLOW = BLUE = ""
    class Style:
        RESET_ALL = ""


class ETLRunner:
    """Orchestrates ETL pipeline execution"""
    
    def __init__(self):
        self.db = DatabaseConnector()
        self.start_time = None
        self.results = {}
        
    def print_header(self, text):
        """Print section header"""
        print(f"\n{Fore.CYAN}{'='*70}")
        print(f"{text}")
        print(f"{'='*70}{Style.RESET_ALL}")
    
    def print_step(self, step, total, text):
        """Print step progress"""
        print(f"\n{Fore.YELLOW}[Step {step}/{total}] {text}{Style.RESET_ALL}")
    
    def run_layer(self, layer_name, layer_func):
        """
        Run a single ETL layer with timing and error handling
        
        Args:
            layer_name: Name of the layer (e.g., "RAW", "STAGING")
            layer_func: Function to execute for this layer
            
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"\n{Fore.BLUE}▶ Running {layer_name} layer...{Style.RESET_ALL}")
        start = time.time()
        
        try:
            result = layer_func()
            elapsed = time.time() - start
            
            print(f"{Fore.GREEN}✅ {layer_name} completed in {elapsed:.2f}s{Style.RESET_ALL}")
            
            # Store results
            self.results[layer_name] = {
                'status': 'success',
                'elapsed': elapsed,
                'result': result
            }
            
            # Print summary if available
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, dict) and 'rows' in value:
                        print(f"  → {key}: {value['rows']:,} rows")
            
            return True
            
        except Exception as e:
            elapsed = time.time() - start
            print(f"{Fore.RED}❌ {layer_name} failed after {elapsed:.2f}s{Style.RESET_ALL}")
            print(f"{Fore.RED}   Error: {str(e)}{Style.RESET_ALL}")
            
            self.results[layer_name] = {
                'status': 'failed',
                'elapsed': elapsed,
                'error': str(e)
            }
            
            return False
    
    def run_full_pipeline(self):
        """
        Run complete ETL pipeline: RAW → STAGING → PRODUCTION
        
        Returns:
            bool: True if all layers successful, False otherwise
        """
        self.start_time = time.time()
        self.print_header("ETL PIPELINE EXECUTION")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Raw Layer
        self.print_step(1, 3, "RAW Layer - Ingest from Parquet files")
        etl_raw = RawLayerETL(self.db)
        if not self.run_layer("RAW", lambda: etl_raw.ingest_all()):
            print(f"\n{Fore.RED}Pipeline stopped due to RAW layer failure{Style.RESET_ALL}")
            return False
        
        # Step 2: Staging Layer
        self.print_step(2, 3, "STAGING Layer - Clean & Transform")
        etl_stg = StagingLayerETL(self.db)
        if not self.run_layer("STAGING", lambda: etl_stg.transform_all()):
            print(f"\n{Fore.RED}Pipeline stopped due to STAGING layer failure{Style.RESET_ALL}")
            return False
        
        # Step 3: Production Layer
        self.print_step(3, 3, "PRODUCTION Layer - Aggregate Metrics")
        etl_prod = ProdLayerETL(self.db)
        if not self.run_layer("PRODUCTION", lambda: etl_prod.build_all()):
            print(f"\n{Fore.RED}Pipeline stopped due to PRODUCTION layer failure{Style.RESET_ALL}")
            return False
        
        return True
    
    def print_summary(self):
        """Print execution summary with statistics"""
        total_time = time.time() - self.start_time
        
        self.print_header("EXECUTION SUMMARY")
        
        print(f"\n{'Layer':<15} {'Status':<15} {'Time':<10} {'Details'}")
        print("-" * 70)
        
        for layer, info in self.results.items():
            if info['status'] == 'success':
                status = f"{Fore.GREEN}✅ SUCCESS{Style.RESET_ALL}"
                details = ""
                if 'result' in info and isinstance(info['result'], dict):
                    total_rows = sum(
                        v.get('rows', 0) 
                        for v in info['result'].values() 
                        if isinstance(v, dict)
                    )
                    if total_rows > 0:
                        details = f"{total_rows:,} rows"
            else:
                status = f"{Fore.RED}❌ FAILED{Style.RESET_ALL}"
                details = info.get('error', '')[:40]
            
            print(f"{layer:<15} {status:<15} {info['elapsed']:>6.2f}s   {details}")
        
        print("-" * 70)
        print(f"{'Total Time':<15} {'':<15} {total_time:>6.2f}s")
        
        # Overall status
        all_success = all(r['status'] == 'success' for r in self.results.values())
        
        print()
        if all_success:
            print(f"{Fore.GREEN}{'='*70}")
            print(f"🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            print(f"{'='*70}{Style.RESET_ALL}")
            print(f"\n{Fore.CYAN}Next steps:{Style.RESET_ALL}")
            print("  - Validate data: make etl-validate")
            print("  - Check quality: make etl-health-check")
            print("  - Open notebooks: make notebook-start")
        else:
            print(f"{Fore.RED}{'='*70}")
            print(f"❌ PIPELINE FAILED!")
            print(f"{'='*70}{Style.RESET_ALL}")
            print(f"\n{Fore.YELLOW}Troubleshooting:{Style.RESET_ALL}")
            print("  - Check logs: make etl-logs")
            print("  - Debug layers: make etl-debug-raw, etl-debug-stg, etl-debug-prod")
            print("  - Review errors above")
        
        return all_success


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='ETL Pipeline Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --full              Run full pipeline (RAW → STAGING → PROD)
  %(prog)s --raw               Run RAW layer only
  %(prog)s --stg               Run STAGING layer only
  %(prog)s --prod              Run PRODUCTION layer only
        """
    )
    
    parser.add_argument('--full', action='store_true', 
                       help='Run full pipeline (default if no options specified)')
    parser.add_argument('--raw', action='store_true', 
                       help='Run raw layer only')
    parser.add_argument('--stg', action='store_true', 
                       help='Run staging layer only')
    parser.add_argument('--prod', action='store_true', 
                       help='Run production layer only')
    
    args = parser.parse_args()
    
    # Initialize runner
    runner = ETLRunner()
    runner.start_time = time.time()
    
    try:
        # Determine what to run
        if args.full or not any([args.raw, args.stg, args.prod]):
            # Run full pipeline
            success = runner.run_full_pipeline()
        else:
            # Run individual layers
            runner.print_header("ETL LAYER EXECUTION")
            print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            success = True
            
            if args.raw:
                etl_raw = RawLayerETL(runner.db)
                success = runner.run_layer("RAW", lambda: etl_raw.ingest_all())
            
            if args.stg and success:
                etl_stg = StagingLayerETL(runner.db)
                success = runner.run_layer("STAGING", lambda: etl_stg.transform_all())
            
            if args.prod and success:
                etl_prod = ProdLayerETL(runner.db)
                success = runner.run_layer("PRODUCTION", lambda: etl_prod.build_all())
        
        # Print summary
        runner.print_summary()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Pipeline interrupted by user{Style.RESET_ALL}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Fore.RED}Fatal error: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()