#!/usr/bin/env python3
"""
Simple test script to verify CDF connection.

Run this script to test if your .env file is configured correctly
and you can connect to CDF.
"""

import sys
from pathlib import Path

# Add utilities to path
sys.path.append('.')

def get_project_root():
    """Get the project root directory."""
    # Start from the current file's directory
    current_file = Path(__file__).resolve()
    return current_file.parent

def main():
    print("🔧 CDF Connection Test")
    print("=" * 50)
    
    # Check if .env file exists in project root
    project_root = get_project_root()
    env_file = project_root / '.env'
    
    if not env_file.exists():
        print(f"✗ .env file not found at: {env_file}")
        print(f"Project root: {project_root}")
        print("\nPlease create a .env file in your project root with the following content:")
        print("-" * 60)
        print("""# CDF Configuration - Replace with your actual credentials
CDF_PROJECT=your-project-name
CDF_CLUSTER=your-cluster
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-client-secret
CDF_TENANT_ID=your-tenant-id
CDF_BASE_URL=https://your-cluster.cognitedata.com

# Performance Test Settings (optional)
DEFAULT_BATCH_SIZE=1000
DEFAULT_ITERATIONS=10
LOG_LEVEL=INFO
""")
        print("-" * 60)
        print("\nReplace the placeholder values with your actual CDF credentials.")
        return False
    
    print(f"✓ .env file found at: {env_file}")
    print()
    
    # Test the connection using the enhanced client_setup
    try:
        from utilities.client_setup import test_connection
        
        # This will display the config and test the connection
        if test_connection():
            print("\n🎉 Success! Your CDF connection is working perfectly.")
            print("\nYou can now:")
            print("✓ Start Jupyter: jupyter notebook")
            print("✓ Open any notebook in the notebooks/ directory")
            print("✓ Run the performance tests")
            print("\nRecommended first notebook:")
            print("  📓 notebooks/data_ingestion/timeseries_ingestion_performance.ipynb")
            return True
        else:
            print("\n❌ Connection failed. Please check your .env file configuration.")
            print("\nTroubleshooting:")
            print("- Verify your credentials are correct")
            print("- Check that your client has the necessary permissions")
            print("- Ensure your cluster name is correct")
            print("- Verify the base URL format")
            return False
            
    except Exception as e:
        print(f"❌ Error during connection test: {e}")
        print("\nThis might be due to:")
        print("- Missing dependencies (run: pip install -r requirements.txt)")
        print("- Incorrect .env file format")
        print("- Network connectivity issues")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 