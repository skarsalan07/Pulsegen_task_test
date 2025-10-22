#!/usr/bin/env python3
"""
Phase 1 Execution Script - Using working historical scraping approach for 2 months
"""

import logging
import sys
import os

# Add current directory to Python path
sys.path.append(os.path.dirname(__file__))

try:
    from src.main import run_phase1_batch_processing
    
    print("Starting Senior AI Engineer Assignment - Phase 1")
    print("App: Swiggy (in.swiggy.android)")
    print("Approach: Historical scraping with daily batch processing")
    print("Timeframe: Last 2 months (60 days)")
    print("=" * 60)
    
    result = run_phase1_batch_processing()
    
    if result is not None and result.get('status') != 'failed':
        print("\nPhase 1 completed successfully!")
        print(f"Processed {result.get('successful', 0)} daily batches")
    else:
        print("\nPhase 1 failed. Check logs for details.")
        
except Exception as e:
    print(f"Failed to start Phase 1: {e}")
    logging.exception("Execution failed")