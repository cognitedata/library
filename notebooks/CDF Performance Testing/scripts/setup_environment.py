#!/usr/bin/env python3
"""
Setup script for CDF performance testing environment.

This script helps users set up their environment for running
CDF performance tests.
"""

import os
import sys
import subprocess
from pathlib import Path


def install_requirements():
    """Install required packages from requirements.txt."""
    print("Installing required packages...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
        )
        print("✓ Requirements installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ Error installing requirements: {e}")
        return False
    return True


def create_env_file():
    """Create a .env file template for CDF configuration."""
    env_file = Path(".env")

    if env_file.exists():
        print("✓ .env file already exists")
        return True

    env_template = """# CDF Configuration
CDF_PROJECT=your-project-name
CDF_CLUSTER=your-cluster
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-client-secret
CDF_TENANT_ID=your-tenant-id
CDF_BASE_URL=https://your-cluster.cognitedata.com

# Performance Test Settings
DEFAULT_BATCH_SIZE=1000
DEFAULT_ITERATIONS=10
LOG_LEVEL=INFO
"""

    try:
        with open(env_file, "w") as f:
            f.write(env_template)
        print("✓ Created .env file template")
        print("  Please edit .env with your CDF credentials")
    except Exception as e:
        print(f"✗ Error creating .env file: {e}")
        return False

    return True


def create_gitignore():
    """Create a .gitignore file to exclude sensitive files."""
    gitignore_file = Path(".gitignore")

    gitignore_content = """# Environment variables
.env

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Jupyter Notebook
.ipynb_checkpoints

# Results and logs
results/
logs/
*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
"""

    try:
        with open(gitignore_file, "w") as f:
            f.write(gitignore_content)
        print("✓ Created .gitignore file")
    except Exception as e:
        print(f"✗ Error creating .gitignore file: {e}")
        return False

    return True


def main():
    """Main setup function."""
    print("Setting up CDF Performance Testing Environment")
    print("=" * 50)

    # Change to project root directory
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    success = True

    # Install requirements
    if not install_requirements():
        success = False

    # Create .env file
    if not create_env_file():
        success = False

    # Create .gitignore
    if not create_gitignore():
        success = False

    print("\n" + "=" * 50)
    if success:
        print("✓ Environment setup completed successfully!")
        print("\nNext steps:")
        print("1. Edit .env file with your CDF credentials")
        print("2. Start Jupyter: jupyter notebook")
        print("3. Open any notebook in the notebooks/ directory")
    else:
        print("✗ Environment setup completed with errors")
        print("Please check the error messages above")

    return success


if __name__ == "__main__":
    main()
