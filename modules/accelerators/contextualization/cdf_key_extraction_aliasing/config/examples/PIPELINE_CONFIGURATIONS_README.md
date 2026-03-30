# Key extraction / aliasing example configs — summary

Combined v1 YAML lives under **`key_extraction/`** and **`aliasing/`**. **`reference/`** holds exhaustive field documentation, not merge demos.

## Usage

1. **Local run**: `main.py --config-path config/examples/key_extraction/<demo>.key_extraction_aliasing.yaml` (or `config/examples/aliasing/aliasing_default.key_extraction_aliasing.yaml`).
2. **CDF**: Inline `key_extraction` / `aliasing` from those files into your workflow task payload.

## Progressive testing (under `key_extraction/`)

1. `regex_pump_tag_simple.key_extraction_aliasing.yaml`
2. `regex_instrument_tag_capture.key_extraction_aliasing.yaml`
3. `fixed_width_single` / `fixed_width_multiline`
4. `token_reassembly`
5. `heuristic_*`
6. `passthrough.key_extraction_aliasing.yaml`

## Aliasing

- `aliasing/aliasing_default.key_extraction_aliasing.yaml`

## Reference

- `reference/config_example_complete.yaml` — flat parameters/data
- `reference/reference_key_extraction_aliasing.yaml` — same content in combined v1 wrap
