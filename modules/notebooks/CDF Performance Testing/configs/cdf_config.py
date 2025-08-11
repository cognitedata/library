"""
Configuration settings for CDF performance testing.

This module contains configuration settings for connecting to CDF
and running performance tests.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class CDFConfig:
    """Configuration class for CDF connection and performance testing."""
    
    def __init__(self):
        # CDF Connection Settings
        self.project = os.getenv('CDF_PROJECT', 'your-project-name')
        self.cluster = os.getenv('CDF_CLUSTER', 'your-cluster')
        self.client_id = os.getenv('CDF_CLIENT_ID', '')
        self.client_secret = os.getenv('CDF_CLIENT_SECRET', '')
        self.tenant_id = os.getenv('CDF_TENANT_ID', '')
        self.base_url = os.getenv('CDF_BASE_URL', f'https://{self.cluster}.cognitedata.com')
        
        # Performance Test Settings
        self.default_batch_size = int(os.getenv('DEFAULT_BATCH_SIZE', '1000'))
        self.default_iterations = int(os.getenv('DEFAULT_ITERATIONS', '10'))
        self.default_warmup_iterations = int(os.getenv('DEFAULT_WARMUP_ITERATIONS', '2'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '5'))
        self.request_timeout = int(os.getenv('REQUEST_TIMEOUT', '30'))
        
        # Test Data Settings
        self.test_data_size = int(os.getenv('TEST_DATA_SIZE', '1000'))
        self.timeseries_test_range = 24 * 60 * 60 * 1000  # 24 hours in milliseconds
        
        # Results Settings
        self.results_base_path = os.getenv('RESULTS_BASE_PATH', 'results')
        self.save_detailed_results = os.getenv('SAVE_DETAILED_RESULTS', 'True').lower() == 'true'
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        
    def get_client_config(self) -> Dict[str, Any]:
        """Get configuration for CDF client initialization."""
        return {
            'project': self.project,
            'base_url': self.base_url,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'tenant_id': self.tenant_id,
            'timeout': self.request_timeout
        }
        
    def get_test_config(self) -> Dict[str, Any]:
        """Get configuration for performance tests."""
        return {
            'batch_size': self.default_batch_size,
            'iterations': self.default_iterations,
            'warmup_iterations': self.default_warmup_iterations,
            'max_workers': self.max_workers,
            'test_data_size': self.test_data_size,
            'timeseries_test_range': self.timeseries_test_range,
            'results_path': self.results_base_path,
            'save_detailed_results': self.save_detailed_results,
            'log_level': self.log_level
        }
        
    def validate_config(self) -> bool:
        """Validate that required configuration is present."""
        required_fields = ['project', 'client_id', 'client_secret', 'tenant_id']
        missing_fields = [field for field in required_fields if not getattr(self, field)]
        
        if missing_fields:
            print(f"Missing required configuration fields: {missing_fields}")
            return False
            
        return True

    def create_cognite_client(self):
        """Create and return a configured Cognite client."""
        from cognite.client import CogniteClient
        from cognite.client.config import ClientConfig
        from cognite.client.credentials import OAuthClientCredentials
        
        if not self.validate_config():
            raise ValueError("Invalid configuration. Please check your .env file.")
        
        # Create credentials object
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=[f"{self.base_url}/.default"]
        )
        
        # Create client config
        config = ClientConfig(
            client_name="CDF-Performance-Tester",
            project=self.project,
            credentials=credentials,
            base_url=self.base_url
        )
        
        # Create the client
        client = CogniteClient(config)
        
        return client


# Global configuration instance
config = CDFConfig() 