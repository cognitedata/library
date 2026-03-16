# Key Extraction Pipeline Configurations - Summary

This directory contains individual extraction pipeline configurations, each isolated from the comprehensive example for easier testing and deployment.

## Created Files

Each extraction rule has been isolated into a standalone extraction pipeline.
Please use these for reference when building custom workflows or testing.

## Common Configuration

All pipelines share the same:
- **Parameters**: debug, verbose, run_all, overwrite flags and RAW database settings
- **Source Views**: 5 example views demonstrating all filter operators (EQUALS, IN, EXISTS, CONTAINSALL, SEARCH)
- **Field Selection Strategy**: merge_all (can be changed to first_match, highest_priority, highest_confidence)
- **Dataset**: ds_key_extraction

## Usage

1. **Deploy a pipeline**: Use the ExtractionPipeline.yaml file to create the pipeline in CDF
2. **Configure the pipeline**: Upload the corresponding config.yaml as the pipeline configuration
3. **Run independently**: Each pipeline can be run and tested separately
4. **Monitor results**: All pipelines write to the same RAW tables for comparison

## Testing Strategy

Test pipelines in this order for progressive complexity:
1. Start with `pump_tag_regex_simple` (simplest)
2. Progress to `instrument_tag_regex_capture` (capture groups)
3. Test fixed-width methods for structured data
4. Explore token reassembly for complex tag formats
5. Experiment with heuristic methods for fuzzy extraction