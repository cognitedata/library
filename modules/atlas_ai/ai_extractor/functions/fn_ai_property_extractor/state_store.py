"""
State Store Handler for the AI Property Extractor.

Manages extraction state using a single CDF RAW row ("_metadata") to track:
- last_processed_cursor: ISO timestamp of last processed instance
- config_version: For detecting when full re-run is needed
- Run statistics

This enables efficient incremental processing by using a high-water mark cursor
to skip already-processed instances in Data Modeling queries.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


class StateStoreHandler:
    """
    Manages extraction state using a single RAW row.
    
    The state store uses a "high-water mark" pattern:
    - Only ONE row ("_metadata") is stored in RAW
    - Contains the cursor (lastUpdatedTime of last processed instance)
    - Data Modeling queries filter by lastUpdatedTime >= cursor to skip processed instances
    
    This approach is memory-efficient and scales to millions of instances because:
    - RAW operations are O(1) - just one row to read/write
    - DM does the heavy lifting with indexed lastUpdatedTime filters
    - No per-instance state storage needed
    """
    
    METADATA_KEY = "_metadata"
    
    def __init__(
        self,
        client: CogniteClient,
        database: str,
        table: str,
        logger=None
    ):
        """
        Initialize the state store handler.
        
        Args:
            client: Authenticated CogniteClient
            database: RAW database name
            table: RAW table name
            logger: Optional logger instance
        """
        self.client = client
        self.database = database
        self.table = table
        self.logger = logger
        self._metadata_cache: Optional[Dict[str, Any]] = None
    
    def _log(self, level: str, message: str) -> None:
        """Log a message if logger is available."""
        if self.logger:
            getattr(self.logger, level.lower(), self.logger.info)(message)
    
    def ensure_exists(self) -> None:
        """
        Create RAW database and table if they don't exist.
        
        This is idempotent - safe to call multiple times.
        """
        try:
            self.client.raw.databases.create(self.database)
            self._log("debug", f"Created RAW database: {self.database}")
        except CogniteAPIError:
            pass  # Already exists
        
        try:
            self.client.raw.tables.create(self.database, self.table)
            self._log("debug", f"Created RAW table: {self.table}")
        except CogniteAPIError:
            pass  # Already exists
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get the metadata with cursor and run info.
        
        Returns empty dict if:
        - Row doesn't exist (first run)
        - RAW service unavailable (fail gracefully)
        
        Uses caching to avoid repeated API calls within the same function run.
        """
        if self._metadata_cache is not None:
            return self._metadata_cache
        
        try:
            row = self.client.raw.rows.retrieve(
                db_name=self.database,
                table_name=self.table,
                key=self.METADATA_KEY
            )
            self._metadata_cache = row.columns if row else {}
            return self._metadata_cache
        except CogniteNotFoundError:
            self._log("debug", "No metadata row found - first run")
            return {}
        except CogniteAPIError as e:
            self._log("warning", f"Could not read state, starting fresh: {e}")
            return {}
    
    def get_cursor(self) -> Optional[str]:
        """
        Get the last processed cursor (ISO timestamp string).
        
        Returns None if no cursor exists (first run or after reset).
        """
        return self.get_metadata().get("last_processed_cursor")
    
    def get_config_version(self) -> Optional[str]:
        """Get the stored config version."""
        return self.get_metadata().get("config_version")
    
    def update_metadata(self, **kwargs) -> None:
        """
        Update metadata row with new values (merges with existing).
        
        Common updates:
        - last_processed_cursor: After each batch
        - config_version: At start of run
        - total_processed: Running count
        - last_run_time: Timestamp
        
        If the update fails, logs a warning but doesn't fail the function.
        Re-processing is safe due to idempotent write modes.
        """
        try:
            self.ensure_exists()
            current = self.get_metadata().copy()
            current.update(kwargs)
            current["last_updated"] = datetime.utcnow().isoformat() + "Z"
            
            self.client.raw.rows.insert(
                db_name=self.database,
                table_name=self.table,
                row=Row(key=self.METADATA_KEY, columns=current)
            )
            self._metadata_cache = current
            self._log("debug", f"Updated metadata: {list(kwargs.keys())}")
            
        except CogniteAPIError as e:
            # Log but don't fail - next run will re-process (safe)
            self._log("warning", f"Cursor update failed, may re-process batch: {e}")
    
    def update_cursor(self, cursor: str, increment_processed: int = 0) -> None:
        """
        Update cursor after successful batch processing.
        
        Args:
            cursor: ISO timestamp string (lastUpdatedTime of latest instance)
            increment_processed: Number of instances processed in this batch
        """
        current = self.get_metadata()
        total = current.get("total_processed", 0) + increment_processed
        
        self.update_metadata(
            last_processed_cursor=cursor,
            total_processed=total,
            last_run_time=datetime.utcnow().isoformat() + "Z"
        )
    
    def reset(self, reason: str = "manual_reset") -> None:
        """
        Reset state for full re-run.
        
        Clears the cursor so next run processes everything.
        Preserves run statistics for auditing.
        
        Args:
            reason: Reason for reset (logged in state)
        """
        self._log("info", f"Resetting state ({reason}) - next run will process all instances")
        self.update_metadata(
            last_processed_cursor=None,
            reset_time=datetime.utcnow().isoformat() + "Z",
            reset_reason=reason
        )
    
    def should_reset_for_config_change(self, current_config_version: Optional[str]) -> bool:
        """
        Check if config version changed, indicating need for full re-run.
        
        Returns True if stored version differs from current.
        Returns False if:
        - First run (no stored version)
        - Current version is None (version tracking disabled)
        
        Args:
            current_config_version: The config version from current config
        """
        if current_config_version is None:
            return False  # Version tracking disabled
            
        stored_version = self.get_config_version()
        if stored_version is None:
            return False  # First run, no reset needed
            
        return stored_version != current_config_version
    
    def initialize_run(
        self,
        config_version: Optional[str] = None,
        force_reset: bool = False
    ) -> Optional[str]:
        """
        Initialize state at start of function run.
        
        This method should be called at the beginning of each function execution.
        It handles:
        - Forced resets (resetState parameter)
        - Config version change detection
        - Normal cursor retrieval for incremental processing
        
        Args:
            config_version: Current config version (None disables version tracking)
            force_reset: If True, reset cursor for full re-run
        
        Returns:
            The cursor to use for DM query (None means process all)
        """
        self.ensure_exists()
        
        # Check for forced reset
        if force_reset:
            self._log("info", "Force reset requested - will process all instances")
            self.reset(reason="force_reset")
            if config_version:
                self.update_metadata(config_version=config_version)
            return None
        
        # Check for config version change
        if self.should_reset_for_config_change(config_version):
            old_version = self.get_config_version()
            self._log("info", f"Config version changed from {old_version} to {config_version} - resetting state")
            self.reset(reason=f"config_version_change:{old_version}->{config_version}")
            self.update_metadata(config_version=config_version)
            return None
        
        # Normal run - return existing cursor
        cursor = self.get_cursor()
        if cursor:
            self._log("debug", f"Resuming from cursor: {cursor}")
        else:
            self._log("debug", "No cursor found - will process all instances")
        
        # Update config version if first run or changed
        if config_version and self.get_config_version() != config_version:
            self.update_metadata(config_version=config_version)
        
        return cursor
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current state store statistics.
        
        Returns dict with:
        - total_processed: Total instances processed across all runs
        - last_run_time: Timestamp of last processing
        - cursor: Current cursor position
        - config_version: Current config version
        """
        metadata = self.get_metadata()
        return {
            "total_processed": metadata.get("total_processed", 0),
            "last_run_time": metadata.get("last_run_time"),
            "cursor": metadata.get("last_processed_cursor"),
            "config_version": metadata.get("config_version"),
            "last_reset": metadata.get("reset_time"),
            "reset_reason": metadata.get("reset_reason")
        }
