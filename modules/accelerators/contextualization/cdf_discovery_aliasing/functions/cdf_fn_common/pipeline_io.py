"""
Shared pipeline I/O vocabulary for flow-canvas-aligned handler chains.

- **input** (``pipeline_input``): what each step feeds into the next handler call.
  - ``cumulative``: use the full working set accumulated so far (default, legacy).
  - ``previous``: use only the output of the immediately preceding handler in the chain
    (or the initial seed for the first handler).

- **output** (``pipeline_output``): how this step updates the working set for downstream
  cumulative steps and for final results.
  - ``merge``: extend the working set (same as ``preserve_original: true`` when not overridden).
  - ``replace``: working set becomes this step's output only (same as ``preserve_original: false``).

Aliasing pathways implement this for ``transform()`` inputs. Key extraction and confidence
match rules accept the same keys in YAML for schema/UI parity; only aliasing applies
``previous`` input semantics in the engine today.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Mapping, Optional, Tuple

PipelineInputMode = Literal["cumulative", "previous"]
PipelineOutputMode = Literal["merge", "replace"]

PIPELINE_INPUT_DEFAULT: PipelineInputMode = "cumulative"
PIPELINE_OUTPUT_DEFAULT: PipelineOutputMode = "merge"


def normalize_pipeline_input(raw: Any) -> PipelineInputMode:
    s = str(raw or PIPELINE_INPUT_DEFAULT).strip().lower()
    if s in ("cumulative", "full", "accumulator", "all"):
        return "cumulative"
    if s in ("previous", "prior", "last", "step"):
        return "previous"
    return PIPELINE_INPUT_DEFAULT


def normalize_pipeline_output(raw: Any) -> PipelineOutputMode:
    s = str(raw or PIPELINE_OUTPUT_DEFAULT).strip().lower()
    if s in ("replace", "overwrite", "only"):
        return "replace"
    return "merge"


def resolve_preserve_original(
    rule_config: Mapping[str, Any],
    *,
    pipeline_output: PipelineOutputMode,
) -> bool:
    """
    ``preserve_original`` explicitly set in YAML wins; otherwise derive from ``output``.
    """
    if "preserve_original" in rule_config:
        return bool(rule_config["preserve_original"])
    return pipeline_output != "replace"


def parse_rule_pipeline_io(rule_config: Mapping[str, Any]) -> Tuple[PipelineInputMode, PipelineOutputMode, bool]:
    """
    Parse ``input`` / ``output`` (and optional ``pipeline_input`` / ``pipeline_output``)
    from a rule dict. Returns (input_mode, output_mode, preserve_original).
    """
    pin = normalize_pipeline_input(
        rule_config.get("input", rule_config.get("pipeline_input"))
    )
    pout = normalize_pipeline_output(
        rule_config.get("output", rule_config.get("pipeline_output"))
    )
    preserve = resolve_preserve_original(rule_config, pipeline_output=pout)
    return pin, pout, preserve


def pipeline_io_dict_for_engine(rule_config: Mapping[str, Any]) -> Dict[str, Any]:
    """Subset to merge into engine rule dicts (aliasing cdf_adapter)."""
    pin, pout, preserve = parse_rule_pipeline_io(rule_config)
    return {
        "pipeline_input": pin,
        "pipeline_output": pout,
        "preserve_original": preserve,
    }
