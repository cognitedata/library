"""
Table generation functions for Operations Summarizer v2.

Produces markdown tables from CDF node instances using the column
definitions in ``config.DATA_TYPE_CONFIGS``.
"""

from typing import List, Any, Optional
from cognite.client import CogniteClient

from config import DATA_TYPE_CONFIGS
from data_retrieval import node_property


def create_markdown_table(
    columns: list,
    instances: List[Any],
    data_type: str,
    client: CogniteClient,
) -> str:
    """Generate markdown table from column definitions and instances."""
    if not instances:
        return ""

    try:
        headers = [col["header"] for col in columns]
        header_row = "| " + " | ".join(headers) + " |\n"
        separator = "|" + "|".join(["---"] * len(headers)) + "|\n"
        parts = [header_row, separator]

        for instance in instances:
            try:
                row = []
                for col in columns:
                    pname = col["property"]
                    default_val = col.get("default", "N/A")
                    formatter = col.get("formatter")

                    if pname in ("externalId", "external_id"):
                        value = instance.external_id
                    else:
                        value = node_property(instance, data_type, pname, client)

                    if value is None or (isinstance(value, (list, str)) and not value):
                        value = default_val

                    if formatter and value != default_val:
                        try:
                            value = formatter(value)
                        except Exception:
                            value = str(value) if value is not None else default_val

                    row.append(str(value))
                parts.append("| " + " | ".join(row) + " |\n")
            except Exception:
                parts.append("| " + " | ".join(["Error"] * len(headers)) + " |\n")

        return "".join(parts)
    except Exception as e:
        return f"Error generating table for {data_type}: {e}"


def generate_table_for_instances(
    data_type: str,
    instances: List[Any],
    client: CogniteClient,
    limit: Optional[int] = None,
) -> str:
    """Generate a markdown table for instances using configuration."""
    if limit:
        instances = instances[:limit]

    try:
        config = DATA_TYPE_CONFIGS.get(data_type)
        if not config:
            return ""

        columns = [
            {
                "header": col.header,
                "property": col.property,
                "formatter": col.formatter,
                "default": col.default,
            }
            for col in config.table_columns
        ]
        return create_markdown_table(columns, instances, data_type, client)

    except Exception as e:
        return f"Error generating table for {data_type}: {e}"
