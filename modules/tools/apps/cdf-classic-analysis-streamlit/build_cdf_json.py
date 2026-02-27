"""Build CDF import JSON for the complete Classic Analysis Streamlit app.

Usage:
  1. From this directory (streamlit_complete/):  python build_cdf_json.py
  2. Import Classic-Analysis-Complete-CDF-source.json into CDF.
"""
import json
from pathlib import Path

DIR = Path(__file__).resolve().parent

def _cdf_patch(app_text: str) -> str:
    old = '''    """Build CogniteClient and project from env/secrets. Returns (client, project) or None."""
    try:
        from cognite.client import CogniteClient, ClientConfig'''
    cdf_patch = '''    """Build CogniteClient and project from env/secrets. Returns (client, project) or None."""
    try:
        from cognite.client import CogniteClient as _C
        _c = _C()
        if getattr(_c.config, "project", None):
            return (_c, _c.config.project)
    except Exception:
        pass
    try:
        from cognite.client import CogniteClient, ClientConfig'''
    if old in app_text and cdf_patch not in app_text:
        return app_text.replace(old, cdf_patch, 1)
    return app_text

def main():
    analysis_py = (DIR / "analysis.py").read_text(encoding="utf-8")
    deep_analysis_py = (DIR / "deep_analysis.py").read_text(encoding="utf-8")
    app_py = (DIR / "app.py").read_text(encoding="utf-8")
    app_py = _cdf_patch(app_py)

    reqs = [
        "streamlit>=1.28.0",
        "cognite-sdk>=7.0.0",
        "pandas>=2.0.0",
    ]

    payload = {
        "entrypoint": "app.py",
        "files": {
            "app.py": {"content": {"text": app_py, "$case": "text"}},
            "analysis.py": {"content": {"text": analysis_py, "$case": "text"}},
            "deep_analysis.py": {"content": {"text": deep_analysis_py, "$case": "text"}},
        },
        "requirements": reqs,
    }
    out_path = DIR / "Classic-Analysis-Complete-CDF-source.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {out_path.name}")


if __name__ == "__main__":
    main()
