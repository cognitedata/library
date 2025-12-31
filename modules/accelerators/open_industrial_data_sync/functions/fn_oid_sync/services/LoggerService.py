import logging
import sys
from datetime import datetime


class CompactLogger:
    """
    Compact logger optimized for 20-line popup windows.
    Uses concise formatting and smart filtering.
    """
    
    def __init__(self, name: str = "OID-Sync", log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear any existing handlers
        self.logger.handlers = []
        
        # Create console handler with compact format
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level.upper()))
        
        # Compact formatter: [TIME] LEVEL: message
        formatter = logging.Formatter(
            fmt='[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.start_time = datetime.now()
        self.stats = {
            "ts_synced": 0,
            "dps_inserted": 0,
            "errors": 0
        }
    
    def info(self, message: str):
        """Log info message"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Log error message"""
        self.logger.error(message)
        self.stats["errors"] += 1
    
    def debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
    
    def progress(self, current: int, total: int, item_type: str = "items"):
        """Log progress in compact format"""
        percent = (current / total * 100) if total > 0 else 0
        self.logger.info(f"Progress: {current}/{total} {item_type} ({percent:.0f}%)")
    
    def update_stats(self, ts_synced: int = 0, dps_inserted: int = 0):
        """Update statistics"""
        self.stats["ts_synced"] += ts_synced
        self.stats["dps_inserted"] += dps_inserted
    
    def summary(self):
        """Print compact summary"""
        duration = (datetime.now() - self.start_time).total_seconds()
        self.logger.info("=" * 50)
        self.logger.info(f"Summary: {duration:.1f}s | "
                        f"Time series: {self.stats['ts_synced']} | "
                        f"Data points: {self.stats['dps_inserted']} | "
                        f"Errors: {self.stats['errors']}")
        self.logger.info("=" * 50)
