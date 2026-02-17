"""
Configuration constants for the CDF Project Health Dashboard.

Used when rendering pre-computed metrics from the Project Health Function.
"""

# Hardcoded constants for CDF link building
CDF_CLUSTER_DOMAIN = ".cognitedata.com"
CDF_WORKSPACE = "data-management"

# Display
MAX_RECENT_RUNS = 5

# Status classifications
SUCCESS_STATUSES = frozenset({"success", "completed", "ready", "seen"})
FAILED_STATUSES = frozenset({"failed", "failure", "error", "timed_out", "timeout"})
RUNNING_STATUSES = frozenset({"running", "in_progress", "transforming"})
PENDING_STATUSES = frozenset({"pending", "queued", "deploying", "scheduled"})

# UI
COLORS = {
    "success": "#00C851",
    "failed": "#FF4444",
    "warning": "#FF8800",
    "pending": "#33B5E5",
    "neutral": "#AAAAAA",
    "empty": "#EEEEEE",
}

STATUS_COLORS = {
    "success": COLORS["success"],
    "completed": COLORS["success"],
    "ready": COLORS["success"],
    "running": COLORS["success"],
    "seen": COLORS["success"],
    "failed": COLORS["failed"],
    "failure": COLORS["failed"],
    "error": COLORS["failed"],
    "timed_out": COLORS["warning"],
    "timeout": COLORS["warning"],
    "pending": COLORS["pending"],
    "queued": COLORS["pending"],
    "deploying": COLORS["pending"],
    "scheduled": COLORS["pending"],
    "terminated": COLORS["neutral"],
    "cancelled": COLORS["neutral"],
    "canceled": COLORS["neutral"],
}

STATUS_EMOJIS = {
    "success": "✅",
    "completed": "✅",
    "ready": "✅",
    "seen": "✅",
    "running": "🔄",
    "in_progress": "🔄",
    "failed": "❌",
    "failure": "❌",
    "error": "❌",
    "timed_out": "⏰",
    "timeout": "⏰",
    "pending": "⏳",
    "queued": "⏳",
    "deploying": "⏳",
    "scheduled": "⏳",
    "terminated": "🛑",
    "cancelled": "🛑",
    "canceled": "🛑",
}
