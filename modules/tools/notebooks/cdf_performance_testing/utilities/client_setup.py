"""
Client setup utilities for CDF performance testing.

This module provides utilities to help users set up their CDF client
by reading configuration from the .env file in the project root.
"""

import os
from pathlib import Path
from dotenv import load_dotenv


def find_project_root():
    """Find the project root by looking for key files/directories."""
    # Start from the current file's directory
    current_dir = Path(__file__).resolve().parent

    # Look for indicators of project root
    indicators = [".env", "requirements.txt", "README.md", "utilities", "notebooks"]

    # Search up the directory tree
    search_dir = current_dir
    for _ in range(5):  # Limit search to 5 levels up
        # Check if this directory contains project indicators
        if any((search_dir / indicator).exists() for indicator in indicators):
            # If we find utilities directory, go up one more level
            if search_dir.name == "utilities":
                search_dir = search_dir.parent
            return search_dir

        # Go up one level
        if search_dir.parent == search_dir:  # Reached filesystem root
            break
        search_dir = search_dir.parent

    # Fallback: assume current working directory
    return Path.cwd()


def get_project_root():
    """Get the project root directory."""
    return find_project_root()


def load_env_variables():
    """Load environment variables from .env file and return them."""
    # Get the project root and look for .env file there
    project_root = get_project_root()
    env_file = project_root / ".env"

    if not env_file.exists():
        return None

    load_dotenv(env_file)

    # Read the CDF configuration variables
    env_vars = {
        "CDF_PROJECT": os.getenv("CDF_PROJECT"),
        "CDF_CLUSTER": os.getenv("CDF_CLUSTER"),
        "CDF_CLIENT_ID": os.getenv("CDF_CLIENT_ID"),
        "CDF_CLIENT_SECRET": os.getenv("CDF_CLIENT_SECRET"),
        "CDF_TENANT_ID": os.getenv("CDF_TENANT_ID"),
        "CDF_BASE_URL": os.getenv("CDF_BASE_URL"),
        "DEFAULT_BATCH_SIZE": os.getenv("DEFAULT_BATCH_SIZE", "1000"),
        "DEFAULT_ITERATIONS": os.getenv("DEFAULT_ITERATIONS", "10"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
    }

    return env_vars


def validate_env_variables(env_vars):
    """Validate that required environment variables are present."""
    if env_vars is None:
        return False, "No .env file found"

    required_vars = [
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "CDF_CLIENT_ID",
        "CDF_CLIENT_SECRET",
        "CDF_TENANT_ID",
    ]
    missing_vars = [var for var in required_vars if not env_vars.get(var)]

    if missing_vars:
        return (
            False,
            f"Missing required environment variables: {', '.join(missing_vars)}",
        )

    return True, "All required variables present"


def display_env_config():
    """Display the current environment configuration (without sensitive data)."""
    env_vars = load_env_variables()

    if env_vars is None:
        project_root = get_project_root()
        env_file_path = project_root / ".env"
        print(f"✗ No .env file found at: {env_file_path}")
        print(f"Project root detected: {project_root}")
        print(f"Current working directory: {Path.cwd()}")
        return False

    print("📋 Current .env configuration:")
    print("-" * 40)

    # Display non-sensitive variables
    safe_vars = {
        "CDF_PROJECT": env_vars.get("CDF_PROJECT"),
        "CDF_CLUSTER": env_vars.get("CDF_CLUSTER"),
        "CDF_BASE_URL": env_vars.get("CDF_BASE_URL"),
        "DEFAULT_BATCH_SIZE": env_vars.get("DEFAULT_BATCH_SIZE"),
        "DEFAULT_ITERATIONS": env_vars.get("DEFAULT_ITERATIONS"),
        "LOG_LEVEL": env_vars.get("LOG_LEVEL"),
    }

    for key, value in safe_vars.items():
        if value:
            print(f"  {key}: {value}")
        else:
            print(f"  {key}: (not set)")

    # Display sensitive variables (masked)
    sensitive_vars = ["CDF_CLIENT_ID", "CDF_CLIENT_SECRET", "CDF_TENANT_ID"]
    for var in sensitive_vars:
        value = env_vars.get(var)
        if value:
            masked_value = (
                value[:4] + "*" * (len(value) - 8) + value[-4:]
                if len(value) > 8
                else "***"
            )
            print(f"  {var}: {masked_value}")
        else:
            print(f"  {var}: (not set)")

    print("-" * 40)

    # Validate configuration
    is_valid, message = validate_env_variables(env_vars)
    if is_valid:
        print("✓ Configuration is valid")
    else:
        print(f"✗ Configuration error: {message}")

    return is_valid


def create_env_template():
    """Create a template .env file content."""
    return """# CDF Configuration - Replace with your actual credentials
CDF_PROJECT=your-project-name
CDF_CLUSTER=your-cluster
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-client-secret
CDF_TENANT_ID=your-tenant-id
CDF_BASE_URL=https://your-cluster.cognitedata.com

# Performance Test Settings (optional - defaults are provided)
DEFAULT_BATCH_SIZE=1000
DEFAULT_ITERATIONS=10
DEFAULT_WARMUP_ITERATIONS=2
MAX_WORKERS=5
REQUEST_TIMEOUT=30
TEST_DATA_SIZE=1000
RESULTS_BASE_PATH=results
SAVE_DETAILED_RESULTS=True
LOG_LEVEL=INFO
"""


def setup_env_file():
    """Guide user through setting up the .env file."""
    project_root = get_project_root()
    env_file = project_root / ".env"

    if env_file.exists():
        print("✓ .env file already exists")
        return display_env_config()

    print("Setting up .env file for CDF credentials...")
    print(f"Please create a .env file at: {env_file}")
    print("=" * 60)
    print(create_env_template())
    print("=" * 60)
    print("\nReplace the placeholder values with your actual CDF credentials:")
    print("- CDF_PROJECT: Your CDF project name")
    print("- CDF_CLUSTER: Your CDF cluster (e.g., 'westeurope-1')")
    print("- CDF_CLIENT_ID: Your application's client ID")
    print("- CDF_CLIENT_SECRET: Your application's client secret")
    print("- CDF_TENANT_ID: Your Azure tenant ID")
    print(
        "- CDF_BASE_URL: Your CDF base URL (usually https://your-cluster.cognitedata.com)"
    )

    return False


def get_client():
    """Get a configured CDF client using environment variables from .env file."""
    try:
        # Load environment variables from .env file
        env_vars = load_env_variables()

        if env_vars is None:
            project_root = get_project_root()
            env_file_path = project_root / ".env"
            print(f"✗ No .env file found at: {env_file_path}")
            print(f"Project root detected: {project_root}")
            setup_env_file()
            return None

        # Validate the loaded variables
        is_valid, message = validate_env_variables(env_vars)
        if not is_valid:
            print(f"✗ Configuration validation failed: {message}")
            setup_env_file()
            return None

        # Import config after loading env variables
        from configs.cdf_config import config

        # Create and return the client
        client = config.create_cognite_client()
        print(f"✓ Successfully connected to CDF project: {client.config.project}")
        return client

    except Exception as e:
        print(f"✗ Error creating CDF client: {e}")
        print("Please check your .env file configuration")
        return None


def test_connection():
    """Test the CDF connection using environment variables from .env file."""
    print("🔍 Testing CDF connection...")

    # First, display the current configuration
    if not display_env_config():
        return False

    # Get the client
    client = get_client()
    if client is None:
        return False

    try:
        # Test the connection by checking the client's project
        print("✓ Connection successful!")
        print(f"  Project: {client.config.project}")

        # Try to access a simple API endpoint to verify connection
        client.iam.token.inspect()
        print("  Token is valid and accessible")
        return True

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()
