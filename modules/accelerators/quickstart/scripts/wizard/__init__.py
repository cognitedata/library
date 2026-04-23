"""
Internal helper package for the Quickstart DP setup wizard.

Modules
-------
_constants      Constants, regexes, dataclasses, YAML paths, and module registries.
_messages       User-facing prompt labels and static message strings.
_file_io        File I/O: backups, line reads/writes, .env parsing.
_yaml           YAML path building, value mutation.
_prompts        Terminal prompts, email validation, change-table display.
_sql            SQL mode switch (COMMON → FILE_ANNOTATION).
_preflight      Toolkit version check, cdf.toml validation, org_dir lookup.
_verification   Post-write cdf build / deploy verification.
_style          ANSI terminal styling (colours, section headers, banners).
"""
