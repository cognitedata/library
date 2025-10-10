import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add the current directory to the path so we can import the modules
sys.path.append(str(Path(__file__).parent))

from handler import handle, run_locally


class TestHandler:
    """Test suite for the handler module."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.mock_client = Mock()
        self.mock_logger = Mock()
        self.mock_config = Mock()
        
    @patch('handler.CogniteFunctionLogger')
    @patch('handler.load_config_parameters')
    @patch('handler.asset_entity_matching')
    def test_handle_success(self, mock_asset_matching, mock_load_config, mock_logger_class):
        """Test successful execution of handle function."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_load_config.return_value = self.mock_config
        mock_asset_matching.return_value = None
        
        # Test data
        test_data = {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": "test_pipeline"
        }
        
        # Execute
        result = handle(test_data, self.mock_client)
        
        # Assertions
        assert result == {"status": "succeeded", "data": test_data}
        mock_logger_class.assert_called_once_with("INFO")
        mock_logger_instance.info.assert_called_once()
        mock_load_config.assert_called_once_with(self.mock_client, test_data)
        mock_logger_instance.debug.assert_called_once_with("Loaded config successfully")
        mock_asset_matching.assert_called_once_with(
            self.mock_client, mock_logger_instance, test_data, self.mock_config
        )
    
    @patch('handler.CogniteFunctionLogger')
    @patch('handler.load_config_parameters')
    @patch('handler.asset_entity_matching')
    def test_handle_with_custom_log_level(self, mock_asset_matching, mock_load_config, mock_logger_class):
        """Test handle function with custom log level."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_load_config.return_value = self.mock_config
        mock_asset_matching.return_value = None
        
        # Test data with custom log level
        test_data = {
            "logLevel": "DEBUG",
            "ExtractionPipelineExtId": "test_pipeline"
        }
        
        # Execute
        result = handle(test_data, self.mock_client)
        
        # Assertions
        assert result == {"status": "succeeded", "data": test_data}
        mock_logger_class.assert_called_once_with("DEBUG")
    
    @patch('handler.CogniteFunctionLogger')
    @patch('handler.load_config_parameters')
    @patch('handler.asset_entity_matching')
    def test_handle_default_log_level(self, mock_asset_matching, mock_load_config, mock_logger_class):
        """Test handle function with default log level."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_load_config.return_value = self.mock_config
        mock_asset_matching.return_value = None
        
        # Test data without log level
        test_data = {
            "ExtractionPipelineExtId": "test_pipeline"
        }
        
        # Execute
        result = handle(test_data, self.mock_client)
        
        # Assertions
        assert result == {"status": "succeeded", "data": test_data}
        mock_logger_class.assert_called_once_with("INFO")  # Default log level
    
    @patch('handler.CogniteFunctionLogger')
    @patch('handler.load_config_parameters')
    @patch('handler.asset_entity_matching')
    def test_handle_config_loading_failure(self, mock_asset_matching, mock_load_config, mock_logger_class):
        """Test handle function when config loading fails."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_load_config.side_effect = Exception("Config loading failed")
        
        # Test data
        test_data = {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": "test_pipeline"
        }
        
        # Execute
        result = handle(test_data, self.mock_client)
        
        # Assertions
        assert result["status"] == "failure"
        assert "Config loading failed" in result["message"]
        mock_logger_instance.error.assert_called_once()
        mock_asset_matching.assert_not_called()
    
    @patch('handler.CogniteFunctionLogger')
    @patch('handler.load_config_parameters')
    @patch('handler.asset_entity_matching')
    def test_handle_asset_matching_failure(self, mock_asset_matching, mock_load_config, mock_logger_class):
        """Test handle function when asset matching fails."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_load_config.return_value = self.mock_config
        mock_asset_matching.side_effect = Exception("Asset matching failed")
        
        # Test data
        test_data = {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": "test_pipeline"
        }
        
        # Execute
        result = handle(test_data, self.mock_client)
        
        # Assertions
        assert result["status"] == "failure"
        assert "Asset matching failed" in result["message"]
        mock_logger_instance.error.assert_called_once()
    
    @patch('handler.CogniteFunctionLogger')
    @patch('builtins.print')
    def test_handle_no_logger_creation_on_exception(self, mock_print, mock_logger_class):
        """Test handle function when logger creation fails."""
        # Setup mocks
        mock_logger_class.side_effect = Exception("Logger creation failed")
        
        # Test data
        test_data = {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": "test_pipeline"
        }
        
        # Execute
        result = handle(test_data, self.mock_client)
        
        # Assertions
        assert result["status"] == "failure"
        assert "Logger creation failed" in result["message"]
        mock_print.assert_called_once()
        assert "[ERROR]" in mock_print.call_args[0][0]


class TestRunLocally:
    """Test suite for the run_locally function."""
    
    def setup_method(self):
        """Setup for each test method."""
        # Set up required environment variables
        self.env_vars = {
            "CDF_PROJECT": "test_project",
            "CDF_CLUSTER": "test_cluster",
            "IDP_CLIENT_ID": "test_client_id",
            "IDP_CLIENT_SECRET": "test_client_secret",
            "IDP_TOKEN_URL": "https://test.token.url"
        }
    
    @patch.dict(os.environ, {}, clear=True)
    def test_run_locally_missing_env_vars(self):
        """Test run_locally function with missing environment variables."""
        with pytest.raises(ValueError) as exc_info:
            run_locally()
        
        assert "Missing one or more env.vars" in str(exc_info.value)
    
    @patch.dict(os.environ, {
        "CDF_PROJECT": "test_project",
        "CDF_CLUSTER": "test_cluster",
        "IDP_CLIENT_ID": "test_client_id",
        "IDP_CLIENT_SECRET": "test_client_secret"
        # Missing IDP_TOKEN_URL
    }, clear=True)
    def test_run_locally_partial_env_vars(self):
        """Test run_locally function with partial environment variables."""
        with pytest.raises(ValueError) as exc_info:
            run_locally()
        
        assert "Missing one or more env.vars" in str(exc_info.value)
        assert "IDP_TOKEN_URL" in str(exc_info.value)
    
    @patch('handler.handle')
    @patch('handler.CogniteClient')
    @patch.dict(os.environ, {
        "CDF_PROJECT": "test_project",
        "CDF_CLUSTER": "test_cluster",
        "IDP_CLIENT_ID": "test_client_id",
        "IDP_CLIENT_SECRET": "test_client_secret",
        "IDP_TOKEN_URL": "https://test.token.url"
    }, clear=True)
    def test_run_locally_success(self, mock_cognite_client, mock_handle):
        """Test successful execution of run_locally function."""
        # Setup mocks
        mock_client_instance = Mock()
        mock_cognite_client.return_value = mock_client_instance
        mock_handle.return_value = {"status": "succeeded"}
        
        # Execute
        run_locally()
        
        # Assertions
        mock_cognite_client.assert_called_once()
        mock_handle.assert_called_once()
        
        # Check that handle was called with the correct parameters
        call_args = mock_handle.call_args
        assert call_args[0][0] == {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": "ep_ctx_timeseries_LOC_SOURCE_entity_matching"
        }
        assert call_args[0][1] == mock_client_instance
    
    @patch('handler.handle')
    @patch('handler.CogniteClient')
    @patch.dict(os.environ, {
        "CDF_PROJECT": "test_project",
        "CDF_CLUSTER": "test_cluster",
        "IDP_CLIENT_ID": "test_client_id",
        "IDP_CLIENT_SECRET": "test_client_secret",
        "IDP_TOKEN_URL": "https://test.token.url"
    }, clear=True)
    def test_run_locally_client_config(self, mock_cognite_client, mock_handle):
        """Test that run_locally creates client with correct configuration."""
        # Setup mocks
        mock_client_instance = Mock()
        mock_cognite_client.return_value = mock_client_instance
        mock_handle.return_value = {"status": "succeeded"}
        
        # Execute
        run_locally()
        
        # Assertions
        mock_cognite_client.assert_called_once()
        client_config = mock_cognite_client.call_args[0][0]
        
        # Check client configuration
        assert client_config.client_name == "Toolkit Entity Matching pipeline"
        assert client_config.base_url == "https://test_cluster.cognitedata.com"
        assert client_config.project == "test_project"
        
        # Check credentials configuration
        credentials = client_config.credentials
        assert credentials.token_url == "https://test.token.url"
        assert credentials.client_id == "test_client_id"
        assert credentials.client_secret == "test_client_secret"
        assert credentials.scopes == ["https://test_cluster.cognitedata.com/.default"]


class TestIntegration:
    """Integration tests for the handler module."""
    
    @patch('handler.asset_entity_matching')
    @patch('handler.load_config_parameters')
    @patch('handler.CogniteFunctionLogger')
    def test_handle_integration_flow(self, mock_logger_class, mock_load_config, mock_asset_matching):
        """Test the complete integration flow of the handle function."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_config = Mock()
        mock_load_config.return_value = mock_config
        mock_asset_matching.return_value = None
        
        # Test data
        test_data = {
            "logLevel": "DEBUG",
            "ExtractionPipelineExtId": "integration_test_pipeline",
            "additionalData": "test_value"
        }
        
        mock_client = Mock()
        
        # Execute
        result = handle(test_data, mock_client)
        
        # Assertions
        assert result["status"] == "succeeded"
        assert result["data"] == test_data
        
        # Verify the complete flow
        mock_logger_class.assert_called_once_with("DEBUG")
        mock_logger_instance.info.assert_called_once()
        mock_load_config.assert_called_once_with(mock_client, test_data)
        mock_logger_instance.debug.assert_called_once_with("Loaded config successfully")
        mock_asset_matching.assert_called_once_with(
            mock_client, mock_logger_instance, test_data, mock_config
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 