"""
Tests for common utility functions.

Tests cover Cognite client setup, file ID extraction, and datetime handling.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add module root to path
module_root = Path(__file__).parent.parent
if str(module_root) not in sys.path:
    sys.path.insert(0, str(module_root))

from common import (
    FILE_ID_PROPERTY_NAMES,
    extract_file_id_from_node,
    extract_uploaded_time_from_node,
    setup_cognite_client,
)


class TestSetupCogniteClient:
    """Test Cognite client setup."""

    @patch.dict(
        os.environ,
        {
            "CDF_PROJECT": "test-project",
            "CDF_CLUSTER": "test-cluster",
            "CDF_URL": "https://test.cognite.com",
            "IDP_CLIENT_ID": "test-client-id",
            "IDP_CLIENT_SECRET": "test-client-secret",
            "IDP_TENANT_ID": "test-tenant-id",
            "IDP_TOKEN_URL": "https://test.idp.com/token",
        },
    )
    @patch("common.CogniteClient")
    @patch("common.OAuthClientCredentials")
    @patch("common.ClientConfig")
    def test_setup_cognite_client_with_all_env_vars(
        self,
        mock_client_config: Mock,
        mock_oauth_credentials: Mock,
        mock_cognite_client: Mock,
    ) -> None:
        """Test setting up Cognite client with all required environment variables."""
        # Arrange
        mock_client_instance = MagicMock()
        mock_cognite_client.return_value = mock_client_instance

        # Act
        client = setup_cognite_client("Test-Client")

        # Assert
        assert client is not None
        mock_oauth_credentials.assert_called_once()
        mock_client_config.assert_called_once()
        mock_cognite_client.assert_called_once()

    @patch.dict(os.environ, {}, clear=True)
    def test_setup_cognite_client_missing_env_vars_exits(self) -> None:
        """Test that missing environment variables causes SystemExit."""
        # Act & Assert
        with pytest.raises(SystemExit):
            setup_cognite_client()

    @patch.dict(
        os.environ,
        {
            "CDF_PROJECT": "test-project",
            "CDF_CLUSTER": "test-cluster",
            "CDF_URL": "https://test.cognite.com",
            "IDP_CLIENT_ID": "test-client-id",
            "IDP_CLIENT_SECRET": "test-client-secret",
            "IDP_TENANT_ID": "test-tenant-id",
            # Missing IDP_TOKEN_URL
        },
    )
    def test_setup_cognite_client_partial_env_vars_exits(self) -> None:
        """Test that partial environment variables causes SystemExit."""
        # Act & Assert
        with pytest.raises(SystemExit):
            setup_cognite_client()


class TestExtractUploadedTimeFromNode:
    """Test extracting uploaded time from CogniteFile node."""

    def test_extract_uploaded_time_from_properties_view_id(self) -> None:
        """Test extracting uploaded time from node.properties with ViewId."""
        # Arrange
        mock_node = MagicMock()
        mock_view_props = {"uploadedTime": "2024-01-01T00:00:00Z"}
        mock_node.properties.get.return_value = mock_view_props

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result == "2024-01-01T00:00:00Z"

    def test_extract_uploaded_time_from_properties_dict(self) -> None:
        """Test extracting uploaded time from node.properties as dict."""
        # Arrange
        mock_node = MagicMock()
        mock_node.properties = {
            "view1": {"uploadedTime": "2024-01-01T00:00:00Z"},
        }

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result == "2024-01-01T00:00:00Z"

    def test_extract_uploaded_time_from_data(self) -> None:
        """Test extracting uploaded time from node.data."""
        # Arrange
        mock_node = MagicMock()
        mock_node.properties = None
        mock_node.data = {
            "view1": {"uploadedTime": "2024-01-01T00:00:00Z"},
        }

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result == "2024-01-01T00:00:00Z"

    def test_extract_uploaded_time_from_sources(self) -> None:
        """Test extracting uploaded time from node.sources."""
        # Arrange
        mock_node = MagicMock()
        mock_node.properties = None
        mock_node.data = None
        mock_source = MagicMock()
        mock_source.properties = {"uploadedTime": "2024-01-01T00:00:00Z"}
        mock_node.sources = [mock_source]

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result == "2024-01-01T00:00:00Z"

    def test_extract_uploaded_time_with_timestamp(self) -> None:
        """Test extracting uploaded time from timestamp value."""
        # Arrange
        mock_node = MagicMock()
        # Timestamp in milliseconds: 2024-01-01 00:00:00 UTC
        timestamp_ms = int(
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
        )
        mock_node.properties = {
            "view1": {"uploadedTime": timestamp_ms},
        }

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result is not None
        assert "2024-01-01" in result

    def test_extract_uploaded_time_with_datetime_object(self) -> None:
        """Test extracting uploaded time from datetime object."""
        # Arrange
        mock_node = MagicMock()
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_node.properties = {
            "view1": {"uploadedTime": dt},
        }

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result is not None
        assert "2024-01-01" in result

    def test_extract_uploaded_time_none_node_returns_none(self) -> None:
        """Test extracting uploaded time from None node returns None."""
        # Act
        result = extract_uploaded_time_from_node(None)

        # Assert
        assert result is None

    def test_extract_uploaded_time_not_found_returns_none(self) -> None:
        """Test extracting uploaded time when not found returns None."""
        # Arrange
        mock_node = MagicMock()
        mock_node.properties = None
        mock_node.data = None
        mock_node.sources = None

        # Act
        result = extract_uploaded_time_from_node(mock_node)

        # Assert
        assert result is None


class TestExtractFileIdFromNode:
    """Test extracting file ID from CogniteFile node."""

    def test_extract_file_id_from_data(self) -> None:
        """Test extracting file ID from node.data."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = {
            "view1": {"id": 12345},
        }

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result == 12345

    def test_extract_file_id_from_properties_view_id(self) -> None:
        """Test extracting file ID from node.properties with ViewId."""
        # Arrange
        mock_node = MagicMock()
        mock_view_props = {"id": 12345}
        mock_node.properties.get.return_value = mock_view_props

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result == 12345

    def test_extract_file_id_from_properties_dict(self) -> None:
        """Test extracting file ID from node.properties as dict."""
        # Arrange
        mock_node = MagicMock()
        mock_node.properties = {
            "view1": {"file": 12345},
        }

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result == 12345

    def test_extract_file_id_from_sources(self) -> None:
        """Test extracting file ID from node.sources."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = None
        mock_node.properties = None
        mock_source = MagicMock()
        mock_source.properties = {"id": 12345}
        mock_node.sources = [mock_source]

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result == 12345

    def test_extract_file_id_with_custom_property_names(self) -> None:
        """Test extracting file ID with custom property names."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = {
            "view1": {"fileId": 12345},
        }

        # Act
        result = extract_file_id_from_node(mock_node, property_names=["fileId"])

        # Assert
        assert result == 12345

    def test_extract_file_id_from_dict_value(self) -> None:
        """Test extracting file ID from dict value."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = {
            "view1": {"id": {"id": 12345}},
        }

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result == 12345

    def test_extract_file_id_from_string_value(self) -> None:
        """Test extracting file ID from string value (converted to int)."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = {
            "view1": {"id": "12345"},
        }

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result == 12345

    def test_extract_file_id_not_found_returns_none(self) -> None:
        """Test extracting file ID when not found returns None."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = None
        mock_node.properties = None
        mock_node.sources = None

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result is None

    def test_extract_file_id_invalid_value_returns_none(self) -> None:
        """Test extracting file ID with invalid value returns None."""
        # Arrange
        mock_node = MagicMock()
        mock_node.data = {
            "view1": {"id": "invalid"},
        }

        # Act
        result = extract_file_id_from_node(mock_node)

        # Assert
        assert result is None


class TestConstants:
    """Test module constants."""

    def test_file_id_property_names_constant(self) -> None:
        """Test FILE_ID_PROPERTY_NAMES constant."""
        # Assert
        assert FILE_ID_PROPERTY_NAMES == ["id", "file"]
        assert isinstance(FILE_ID_PROPERTY_NAMES, list)
