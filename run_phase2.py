#!/usr/bin/env python3
"""
Phase 2 Execution Script
Run after Phase 1 completion
"""

import logging
import sys
import os

# Add current directory to Python path
sys.path.append(os.path.dirname(__file__))

try:
    from src.main_phase2 import run_phase2
    
    print("🚀 Starting Phase 2: AI Agentic Topic Processing")
    print("📝 Processing: Daily batches → LLM Topic Extraction → Semantic Consolidation")
    print("⏰ Estimated time: 10-30 minutes (depending on reviews)")
    print("=" * 60)
    
    result = run_phase2()
    
    if result is not None:
        print("\n✅ Phase 2 completed successfully!")
        print("➡️ Next: Run Phase 3 for trend analysis")
    else:
        print("\n❌ Phase 2 failed. Check logs for details.")
        
except Exception as e:
    print(f"❌ Failed to start Phase 2: {e}")
    logging.exception("Execution failed")