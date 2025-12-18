"""
ETL Runner - Orchestrate the full ETL pipeline

Usage:
    python etl_runner.py --full      # Run full ETL pipeline
    python etl_runner.py --raw       # Run only raw layer
    python etl_runner.py --stg       # Run only staging layer
    python etl_runner.py --prod      # Run only production layer
"""

import argparse
import logging
from datetime import datetime

from etl_raw import RawLayerETL
from etl_stg import StagingLayerETL
from etl_prod import ProdLayerETL

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLRunner:
    """Orchestrate the ETL pipeline"""
    
    def __init__(self):
        self.raw_etl = RawLayerETL()
        self.stg_etl = StagingLayerETL()
        self.prod_etl = ProdLayerETL()
    
    def run_raw(self, full_refresh: bool = False) -> dict:
        """Run raw layer ETL"""
        logger.info("\n" + "="*70)
        logger.info("STEP 1/3: RAW LAYER")
        logger.info("="*70)
        
        if full_refresh:
            self.raw_etl.truncate_all()
        
        return self.raw_etl.ingest_all(incremental=not full_refresh)
    
    def run_stg(self) -> dict:
        """Run staging layer ETL"""
        logger.info("\n" + "="*70)
        logger.info("STEP 2/3: STAGING LAYER")
        logger.info("="*70)
        
        return self.stg_etl.transform_all()
    
    def run_prod(self) -> dict:
        """Run production layer ETL"""
        logger.info("\n" + "="*70)
        logger.info("STEP 3/3: PRODUCTION LAYER")
        logger.info("="*70)
        
        return self.prod_etl.build_all()
    
    def run_full_pipeline(self, full_refresh: bool = False) -> dict:
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        
        logger.info("\n" + "#"*70)
        logger.info("ETL PIPELINE - STARTING FULL RUN")
        logger.info(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
        logger.info(f"Start Time: {start_time}")
        logger.info("#"*70)
        
        results = {
            'start_time': start_time.isoformat(),
            'mode': 'full_refresh' if full_refresh else 'incremental'
        }
        
        try:
            # Step 1: Raw Layer
            results['raw'] = self.run_raw(full_refresh)
            
            # Step 2: Staging Layer
            results['stg'] = self.run_stg()
            
            # Step 3: Production Layer
            results['prod'] = self.run_prod()
            
            results['status'] = 'SUCCESS'
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            results['status'] = 'FAILED'
            results['error'] = str(e)
            raise
        
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            results['end_time'] = end_time.isoformat()
            results['duration_seconds'] = duration
            
            logger.info("\n" + "#"*70)
            logger.info(f"ETL PIPELINE - {results['status']}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("#"*70)
        
        return results


def main():
    parser = argparse.ArgumentParser(description='ETL Pipeline Runner')
    parser.add_argument('--full', action='store_true',
                        help='Run full pipeline (raw -> stg -> prod)')
    parser.add_argument('--raw', action='store_true',
                        help='Run only raw layer')
    parser.add_argument('--stg', action='store_true',
                        help='Run only staging layer')
    parser.add_argument('--prod', action='store_true',
                        help='Run only production layer')
    parser.add_argument('--refresh', action='store_true',
                        help='Full refresh (truncate before loading)')
    args = parser.parse_args()
    
    runner = ETLRunner()
    
    if args.raw:
        result = runner.run_raw(full_refresh=args.refresh)
        print(f"\nRaw Layer Result: {result}")
    
    elif args.stg:
        result = runner.run_stg()
        print(f"\nStaging Layer Result: {result}")
    
    elif args.prod:
        result = runner.run_prod()
        print(f"\nProduction Layer Result: {result}")
    
    else:  # Default: full pipeline
        result = runner.run_full_pipeline(full_refresh=args.refresh)
        print(f"\nPipeline Result: {result}")


if __name__ == '__main__':
    main()
