"""Derive reference-index RAW table names from key-extraction state table names (workflow / local runner parity)."""


def reference_index_raw_table_from_key_extraction_table(raw_table_key: str) -> str:
    """
    Map ``raw_table_key`` to the inverted-index RAW table used by ``fn_dm_reference_index``.

    Must match ``local_runner`` and deployed workflow naming (e.g. ``key_extraction_state`` → ``reference_index``).
    """
    k = (raw_table_key or "").strip()
    if not k:
        return "reference_index"
    if k.endswith("_key_extraction_state"):
        return k[: -len("_key_extraction_state")] + "_reference_index"
    if k == "key_extraction_state":
        return "reference_index"
    return f"{k}_reference_index"
