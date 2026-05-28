"""Derive inverted-index RAW table names from key-extraction state table names."""


def inverted_index_raw_table_from_key_extraction_table(raw_table_key: str) -> str:
    k = (raw_table_key or "").strip()
    if not k:
        return "inverted_index"
    if k.endswith("_key_extraction_state"):
        return k[: -len("_key_extraction_state")] + "_inverted_index"
    if k == "key_extraction_state":
        return "inverted_index"
    return f"{k}_inverted_index"
