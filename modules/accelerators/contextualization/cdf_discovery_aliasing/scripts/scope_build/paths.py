"""Module-root-relative paths for CDF workflow YAML build output vs authoring templates."""

from pathlib import Path

# Authoring YAML under workflow_template/ (workflow shells + scope body) for --build (read-only inputs).
WORKFLOW_TEMPLATE_REL = Path("workflow_template")
# Generated Workflow / WorkflowVersion / WorkflowTrigger YAML (Toolkit deploy targets).
WORKFLOW_ARTIFACTS_REL = Path("workflows")
