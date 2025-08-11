#!/usr/bin/env python3
"""
Test runner script for the entity matching function.
"""
import subprocess
import sys
from pathlib import Path

def run_tests():
    """Run all tests for the entity matching function."""
    print("Running tests for entity matching function...")
    
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    
    # Run pytest with verbose output
    cmd = [
        sys.executable, "-m", "pytest", 
        "test_handler.py", 
        "-v", 
        "--tb=short",  # Shorter traceback format
        "--color=yes"   # Colored output
    ]
    
    try:
        result = subprocess.run(cmd, cwd=script_dir, check=False)
        
        if result.returncode == 0:
            print("\n✅ All tests passed!")
        else:
            print(f"\n❌ Tests failed with return code: {result.returncode}")
            
        return result.returncode
        
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(run_tests()) 