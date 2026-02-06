# Marimo Best Practices & Learnings

A collection of practical learnings from building the TSJM Analysis notebook with marimo and cursor.

## Table of Contents

1. [Cell Output Rendering](#1-cell-output-rendering)
2. [Form Gating with Submit Button](#2-form-gating-with-submit-button)
3. [Avoiding Multiple Definition Errors](#3-avoiding-multiple-definition-errors)
4. [Using Setup Cells for Imports](#4-using-setup-cells-for-imports)
5. [Progress Bars and Dynamic Output](#5-progress-bars-and-dynamic-output)
6. [Altair Chart Interactions](#6-altair-chart-interactions) - *hover, click, brush, zoom, multi-line tooltips*
7. [Prerequisites Across Cells](#7-prerequisites-across-cells)
8. [No Early Returns in Cells](#8-no-early-returns-in-cells)
9. [Exception Handling](#9-exception-handling)
10. [UI Element Tips](#10-ui-element-tips) - *callout styling, reusable UI factories*
11. [Testing in Marimo](#11-testing-in-marimo)
12. [Naming Cell Functions](#12-naming-cell-functions)
13. [Polars Schema Inference Issues](#13-polars-schema-inference-issues)

---

## 1. Cell Output Rendering

### Problem
Conditional outputs don't render properly:

```python
# ❌ BAD: Only one branch renders
if condition:
    mo.md("## True")
else:
    mo.md("## False")
```

### Solution
Assign to a local variable and display at the end:

```python
# ✅ GOOD: Always renders correctly
if condition:
    _output = mo.md("## True")
else:
    _output = mo.md("## False")

_output  # Display at end of cell
```

### Why
Marimo cells can only have one output. The last expression is rendered. Using a variable ensures consistent output regardless of which branch executes.

---

## 2. Form Gating with Submit Button

### Problem
UI element changes trigger immediate cell re-runs, which can cause unwanted side effects (e.g., reconnecting to CDF on every keystroke).

### Solution
Use `mo.ui.batch().form()` to gate changes until submit:

```python
# Create a form that requires submission
config_form = mo.ui.batch(
    mo.md("""
### Configuration
**Customer:** {customer}
**Path:** {path}
    """),
    {
        "customer": mo.ui.dropdown(options=["a", "b", "c"]),
        "path": mo.ui.text(value="/default/path"),
    },
).form(submit_button_label="🔗 Connect")

config_form  # Renders form with submit button
```

Then extract values in another cell:

```python
if config_form.value is None:
    _output = mo.md("👆 Fill in form and click Submit")
else:
    customer = config_form.value["customer"]
    path = config_form.value["path"]
    _output = mo.md(f"✅ Submitted: {customer}")

_output
return customer, path
```

See: [marimo composite elements docs](https://docs.marimo.io/guides/interactivity/#composite-elements)

---

## 3. Avoiding Multiple Definition Errors

### Problem
```
MultipleDefinitionError: This app can't be run because it has 
multiple definitions of the name 'result'
```

### Solution
Prefix local variables with `_` to exclude them from marimo's dependency graph:

```python
# ❌ BAD: 'result' might conflict with other cells
result = compute_something()
mo.ui.table(result)

# ✅ GOOD: '_result' is local to this cell
_result = compute_something()
mo.ui.table(_result)
```

### Common Conflicts
- `selected`, `result`, `data`, `df`, `chart`
- Loop variables: `row`, `item`, `i`
- Temporary variables: `min_date`, `max_date`
- **Context manager variables**: `progress` (from `mo.status.progress_bar()`), `file` (from `open()`)

### Context Manager Variables
**Always prefix context manager variables** to avoid conflicts across cells:

```python
# ❌ BAD: Will conflict if used in multiple cells
with mo.status.progress_bar(total=100) as progress:
    progress.update(50)

# ✅ GOOD: Prefixed to avoid conflicts
with mo.status.progress_bar(total=100) as _progress:
    _progress.update(50)
```

This is especially important for export functions (`run_tsjm_export`, `run_wfj_export`) that both use progress bars.

### Data Source Naming

Variables without `_` prefix become **data sources** in Marimo's UI (visible in the data panel). Give them descriptive names that indicate their purpose:

```python
# ❌ BAD: Generic name "selected" appears in data sources
selected = chart_element.value
selected_rows = selected.to_dicts()

# ✅ GOOD: Descriptive prefix, or use _ for truly local vars
_concurrency_selection = chart_element.value
_selected_rows = _concurrency_selection.to_dicts()

# ✅ ALSO GOOD: If you want it visible in data sources, name it clearly
concurrency_chart_selection = chart_element.value  # Shows as "concurrency_chart_selection"
```

### Examples from this notebook:
- `_client_ready`, `_status_indicator`, `_status_text` in `create_config_form`
- `_export_output`, `_output_file` in `run_tsjm_export` and `run_wfj_export`
- `_progress` in `run_tsjm_export` and `run_wfj_export` (progress bar context manager)
- `_threading`, `_traceback` for local imports to avoid conflicts
- `_chart_df`, `_wide_df` in chart rendering cells

---

## 4. Using Setup Cells for Imports

### Problem
When imports are scattered across multiple cells, Marimo has to track dependencies between cells for each import. This creates unnecessary complexity in the dependency graph and can slow down execution.

```python
# ❌ BAD: Imports scattered across cells
@app.cell
def import_marimo_and_title():
    import marimo as mo
    return (mo,)

@app.cell
def import_core_libraries():
    import polars as pl
    import altair as alt
    return Path, alt, datetime, json, pl

@app.cell
def some_function(mo, pl, alt):
    # Function needs imports as parameters
    chart = alt.Chart(...)
    return chart
```

### Solution
Use `app.setup` block to load all imports once at the top level. Imports become globally available to all cells without needing to pass them as parameters.

```python
import marimo

app = marimo.App()

with app.setup(hide_code=True):
    # Standard library imports
    import json
    import os
    import time
    from pathlib import Path
    import datetime

    # Third-party library imports
    import marimo as mo
    import polars as pl
    import altair as alt
    import pandas as pd
    
    # Configure libraries (runs once)
    alt.renderers.set_embed_options(actions=False)

@app.cell
def some_function():
    # Imports are available globally - no need to pass as parameters
    chart = alt.Chart(...)
    return chart
```

### Benefits

1. **Simpler Dependency Graph**: Marimo doesn't need to track import dependencies between cells
2. **Cleaner Cell Signatures**: No need to pass imports as parameters to every function
3. **Better Performance**: Imports loaded once instead of potentially multiple times
4. **Follows Best Practices**: Setup block is designed for one-time initialization

### What Goes in Setup?

- **All imports** (standard library and third-party)
- **Library configuration** (e.g., `alt.renderers.set_embed_options()`)
- **Monkeypatches** or other one-time setup code
- **Constants** that don't change

### What Stays in Cells?

- **Conditional imports** inside functions (e.g., `import pandas as _pd` inside a try-except)
- **Data processing logic**
- **UI creation**
- **Business logic**

### Example from This Notebook

```python
with app.setup(hide_code=True):
    # Standard library imports
    import json
    import os
    import re
    import time
    import traceback
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pathlib import Path
    from typing import Any
    import datetime

    # Third-party library imports
    import marimo as mo
    import polars as pl
    import altair as alt
    import pandas as pd
    from dotenv import load_dotenv
    from cognite.client.credentials import OAuthInteractive
    from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

    # Configure Altair
    alt.renderers.set_embed_options(actions=False)

    # SDK monkeypatch (one-time setup)
    OAuthInteractive._refresh_access_token = _refresh_access_token_patch

@app.cell
def create_chart():
    # mo, alt, pl are all available globally - no parameters needed!
    chart = alt.Chart(data).mark_line()
    return chart
```

### Reference

- [Marimo Documentation: Reusing Functions - Setup Cells](https://docs.marimo.io/guides/reusing_functions/#creating-a-top-level-function-or-class)

---

## 5. Progress Bars and Dynamic Output

### Problem
Progress bar shows only at the end, not during execution.

### Solution
Pass the iterator directly to `mo.status.progress_bar()`, don't wrap in `list()`:

```python
# ❌ BAD: list() blocks until all futures complete
completed = list(as_completed(futures))
for future in mo.status.progress_bar(completed):
    ...

# ✅ GOOD: Pass iterator directly with total
for future in mo.status.progress_bar(
    as_completed(futures),
    total=len(futures),
    title="Processing",
):
    ...
```

### Appending Output After Progress Bar
Use `mo.output.append()` to add content after the progress bar completes:

```python
for item in mo.status.progress_bar(items):
    process(item)

# Add summary after progress bar
mo.output.append(mo.md("✅ **Complete!** Processed 100 items."))
```

---

## 6. Altair Chart Interactions

### Using `mo.ui.altair_chart()`
Wrap Altair charts for selection support:

```python
chart = alt.Chart(df).mark_bar().encode(x="date:T", y="value:Q")
chart_element = mo.ui.altair_chart(chart)
chart_element  # Display
```

Access selection in another cell:
```python
selected = chart_element.value  # DataFrame of selected points
```

### Selection Types
```python
# Brush (drag to select range)
brush = alt.selection_interval(encodings=["x"], name="brush")

# Click (select point) - nearest=True snaps to closest point
click = alt.selection_point(on="click", nearest=True, name="click")

# Hover (for tooltips that follow cursor)
hover = alt.selection_point(on="pointerover", nearest=True, name="hover")
```

### Hover Tooltips

**Single-line chart** - Add tooltip to encoding:
```python
base = alt.Chart(data).encode(
    x=alt.X("time:T", title="Time"),
    y=alt.Y("value:Q", title="Value"),
    tooltip=[
        alt.Tooltip("time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
        alt.Tooltip("value:Q", title="Value"),
    ],
)
```

**Multi-line chart** - Use a vertical rule with nearest-x selection:
```python
# Hover selection snaps to nearest x-value (date), not nearest point
_hover_nearest = alt.selection_point(
    name="hover",
    on="pointerover",
    nearest=True,
    fields=["date"],  # Snap to date, not individual points
    empty=False,
)

# Base line chart (no tooltip here)
_line_chart = alt.Chart(data).mark_line().encode(
    x="date:T", y="total:Q", color="metric:N"
)

# Points layer - shows all metrics at hovered date
_points = (
    alt.Chart(data)
    .mark_point(filled=True, size=60)
    .encode(
        x="date:T", y="total:Q", color="metric:N",
        opacity=alt.condition(_hover_nearest, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
            alt.Tooltip("metric:N", title="Metric"),
            alt.Tooltip("total:Q", title="Total", format=",d"),
        ],
    )
    .add_params(_hover_nearest)
)

# Vertical rule at hovered date
_rule = (
    alt.Chart(data)
    .mark_rule(color="gray", strokeDash=[3, 3])
    .encode(x="date:T", opacity=alt.condition(_hover_nearest, alt.value(0.7), alt.value(0)))
    .transform_filter(_hover_nearest)
)

chart = _line_chart + _points + _rule
```

### Combining Hover + Click + Brush

For maximum interactivity, combine multiple selection types:

```python
# 1. Brush selection (drag to select time range)
brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)

# 2. Click selection (click on a point)
click_select = alt.selection_point(on="click", nearest=True, name="click", empty=False)

# 3. Zoom selection (Ctrl+Shift+scroll to zoom x-axis)
zoom = alt.selection_interval(
    bind="scales",
    encodings=["x"],
    zoom="wheel![event.ctrlKey && event.shiftKey]",
    translate=False,  # Disable drag-to-pan (conflicts with brush)
)

# Combine for highlighting
combined_selection = brush | click_select

base = alt.Chart(data).encode(
    x=alt.X("time:T"),
    y=alt.Y("value:Q"),
    tooltip=[alt.Tooltip("time:T"), alt.Tooltip("value:Q")],
)

line = base.mark_line()
points = (
    base.mark_point(filled=True, size=5)
    .encode(
        opacity=alt.condition(combined_selection, alt.value(1), alt.value(0.4)),
        color=alt.condition(combined_selection, alt.value("red"), alt.value("#1f77b4")),
        size=alt.condition(click_select, alt.value(30), alt.value(5)),
    )
    .add_params(brush)
    .add_params(click_select)
    .add_params(zoom)
)

chart = (line + points).properties(width="container", height=400)
```

**Interaction summary:**
- **Hover** → Shows tooltip with time and value
- **Click** → Selects point (turns red, enlarges)
- **Drag** → Brush selection for time range
- **Ctrl+Shift+scroll** → Zoom x-axis

### Initial Selection State with `empty=False`

**Problem:** By default, `alt.condition()` treats "no selection" as "everything selected". This causes all points to render with the "selected" style on initial load:

```python
# ❌ BAD: All points appear selected (red/large) when chart first loads
brush = alt.selection_interval(encodings=["x"], name="brush")
points = base.mark_point().encode(
    color=alt.condition(brush, alt.value("red"), alt.value("blue")),
)
# Result: All points are red until user makes a selection
```

**Solution:** Add `empty=False` to selections:

```python
# ✅ GOOD: Points start unselected, only highlight when user selects
brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)
click = alt.selection_point(on="click", nearest=True, name="click", empty=False)

points = base.mark_point().encode(
    color=alt.condition(brush | click, alt.value("red"), alt.value("blue")),
)
# Result: All points are blue initially, turn red when selected
```

### ⚠️ Avoid Combining Brush with Zoom

**Problem:** Combining `selection_interval` for brush with `selection_interval(bind="scales")` for zoom causes the brush selection to be offset after zooming.

```python
# ❌ BAD: Two selection_intervals conflict
brush = alt.selection_interval(encodings=["x"])
zoom = alt.selection_interval(bind="scales", encodings=["x"], zoom="wheel![...]")
chart.add_params(brush).add_params(zoom)  # Brush offset after zoom!

# ✅ GOOD: Use only brush selection
brush = alt.selection_interval(encodings=["x"])
chart.add_params(brush)
```

**Better approach:** Use UI controls (sliders) for zoom/pan:

```python
# Zoom slider: 100% = full range, 10% = 10% of range
zoom_slider = mo.ui.slider(start=10, stop=100, value=100, step=10, label="Zoom %")

# Pan slider: 0 = left edge, 100 = right edge
pan_slider = mo.ui.slider(start=0, stop=100, value=50, step=5, label="Pan")

# Calculate effective range in a separate cell
zoom_pct = zoom_slider.value / 100.0
visible_days = max(1, int(total_days * zoom_pct))
offset_days = int((total_days - visible_days) * (pan_slider.value / 100.0))
```

### Avoiding OutOfBoundsError
When chart data changes (e.g., switching metrics), old selection indices become invalid:

```python
# ❌ BAD: Polars DataFrame can cause indexing errors
chart = alt.Chart(polars_df)

# ✅ GOOD: Convert to pandas for stable indexing
chart = alt.Chart(polars_df.to_pandas())
```

### Datetime Precision Mismatch
Marimo's altair_chart filtering uses microseconds:

```python
# ❌ BAD: Milliseconds don't match filter
chart_data = df.with_columns(pl.col("time").cast(pl.Datetime("ms")))

# ✅ GOOD: Cast to microseconds
chart_data = df.with_columns(pl.col("time").cast(pl.Datetime("us")))
```

### ⚠️ Marimo Limitations with Selection Types

**Problem:** `mo.ui.altair_chart()` captures ALL `selection_point` and `selection_interval` params added to a chart, causing state conflicts when combining click and brush selections.

**What works reliably:**
```python
# ✅ Single interval selection (brush/drag) - RECOMMENDED
brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)
chart.add_params(brush)
```

**What causes issues:**
```python
# ❌ Combining click + brush causes state conflicts in marimo
brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)
click = alt.selection_point(on="click", nearest=True, name="click", empty=False)
chart.add_params(brush).add_params(click)  # State gets confused!
```

**What happens:**
- Visual selection works (points turn red)
- But `chart_element.value` returns stale/incorrect data
- Switching between click and brush corrupts selection state
- Table updates become unreliable

**Recommended pattern for interactive charts:**
```python
# Single brush selection for range select
brush = alt.selection_interval(encodings=["x"], name="brush", empty=False)

# Zoom (optional, uses different event trigger)
zoom = alt.selection_interval(
    bind="scales",
    encodings=["x"],
    zoom="wheel![event.ctrlKey && event.shiftKey]",
    translate=False,
)

# Points with tooltip (hover directly over point)
points = (
    alt.Chart(data)
    .mark_point(filled=True, size=50)
    .encode(
        x="time:T", y="value:Q",
        opacity=alt.condition(brush, alt.value(1), alt.value(0.6)),
        tooltip=[alt.Tooltip("time:T"), alt.Tooltip("value:Q")],
    )
    .add_params(brush)
    .add_params(zoom)
)
```

**Note:** Hover snapping (`selection_point` with `on="pointerover"`) also triggers marimo updates, even with different names. Use native tooltips (hover directly over points) for stable behavior.

### Debug Cells for Edit Mode Only

Show debug information only when editing, hide in app/run mode:

```python
@app.cell(hide_code=True)
def debug_selection_state(chart_element, mo):
    """Debug cell - only visible in edit mode."""
    _is_edit_mode = mo.app_meta().mode == "edit"

    if not _is_edit_mode:
        _output = None  # Hidden in app/run mode
    else:
        _selection = chart_element.value
        _output = mo.callout(
            mo.md(f"**Selection:** {len(_selection) if _selection is not None else 0} rows"),
            kind="info",
        )

    _output
    return
```

**Available modes from `mo.app_meta().mode`:**
- `"edit"` - Editing in marimo editor
- `"run"` - Running as an app (`marimo run`)
- `"script"` - Running as a script
- `"test"` - Running in pytest
- `None` - Mode could not be determined

---

## 7. Prerequisites Across Cells

### Problem
A cell needs data from previous cells that might not be ready yet.

### Solution
Check for `None` and provide feedback:

```python
@app.cell
def _(active_client, selected_project, mo):
    # Check prerequisites
    if active_client is None or selected_project is None:
        _output = mo.callout(
            mo.md("⚠️ **Select a project in Chapter 1 first**"),
            kind="warn",
        )
    else:
        # Proceed with actual logic
        _output = mo.md(f"✅ Connected to {selected_project}")
    
    _output
```

### Disable Buttons Until Ready
```python
export_button = mo.ui.run_button(
    label="🚀 Start Export",
    disabled=not (active_client is not None and selected_project is not None),
)
```

---

## 8. No Early Returns in Cells

### Problem
Using `return` in the middle of a marimo cell causes a SyntaxError:

```python
# ❌ BAD: SyntaxError - 'return' outside function
@app.cell
def process_data(data, mo):
    if data is None:
        mo.output.append(mo.md("⚠️ No data"))
        return  # SyntaxError: 'return' outside function
    
    result = compute(data)
    return (result,)
```

Error message:
```
This cell wasn't run because it has errors
SyntaxError: 'return' outside function
```

### Why This Happens
Marimo cells use a special execution model where only the **final `return` statement** is recognized as part of the cell's function signature. Any `return` in the middle of the cell is treated as if it's outside a function context.

### Solution
Structure logic to always reach a **single `return` at the end**:

```python
# ✅ GOOD: Initialize defaults, use if/else, single return at end
@app.cell
def process_data(data, mo):
    result = None  # Default value
    
    if data is None:
        _output = mo.md("⏳ Waiting for data...")
    else:
        result = compute(data)
        _output = mo.md(f"✅ Result: {result}")
    
    _output  # Display at end
    return (result,)
```

### For Complex Logic with Multiple Exit Points
Use nested `if/else` instead of early returns:

```python
# ✅ GOOD: All branches reach the same endpoint
@app.cell
def run_export(transformations, mo):
    export_result = {}
    
    if len(transformations) == 0:
        # Handle empty case
        export_result = {"status": "empty", "rows": 0}
        _output = mo.md("⚠️ No transformations found")
    else:
        # Handle normal case
        rows = process_transformations(transformations)
        if rows == 0:
            export_result = {"status": "empty", "rows": 0}
            _output = mo.md("⚠️ No jobs found")
        else:
            export_result = {"status": "success", "rows": rows}
            _output = mo.md(f"✅ Exported {rows} rows")
    
    mo.output.append(_output)
    return (export_result,)
```

---

## 9. Exception Handling

### Don't Silently Pass
```python
# ❌ BAD: Silent failure hides data quality issues
try:
    data = json.loads(json_str)
except (json.JSONDecodeError, TypeError):
    pass

# ✅ GOOD: Track and report errors
_parse_errors = []
try:
    data = json.loads(json_str)
except (json.JSONDecodeError, TypeError) as e:
    _parse_errors.append({
        "row_id": row_id,
        "error": str(e),
        "preview": json_str[:100],
    })

# Later: Show errors to user
if _parse_errors:
    mo.callout(
        mo.md(f"⚠️ **{len(_parse_errors)} rows had errors**"),
        kind="warn",
    )
```

### Returning None is OK for Aggregations
```python
def extract_metric(json_str, metric_name):
    """Extract metric - None is valid for aggregation filtering."""
    if not json_str:
        return None
    try:
        return json.loads(json_str).get(metric_name)
    except (json.JSONDecodeError, TypeError):
        return None  # Filtered out by .is_not_null()
```

---

## 10. UI Element Tips

### Searchable Dropdown
```python
metric_selector = mo.ui.dropdown(
    options=sorted(metrics, key=str.lower),  # Case-insensitive sort
    searchable=True,  # Enable type-to-filter
    label="Select Metric",
)
```

### Table with Single Selection
```python
project_selector = mo.ui.table(
    pd.DataFrame({"Project": projects, "Default": ["✓" if p == default else "" for p in projects]}),
    selection="single",
)

# Extract selection
if project_selector.value is not None and not project_selector.value.empty:
    selected = project_selector.value.iloc[0]["Project"]
```

### Run Button vs Checkbox
```python
# Use run_button for one-time actions
export_button = mo.ui.run_button(label="🚀 Start Export")

# Check with .value
if export_button.value:
    do_export()
```

### Consistent Callout Styling for Hints

Use `mo.callout()` with a consistent visual pattern for all user hints and status messages. This creates a polished, professional UX and helps users quickly understand what action is needed.

**Pattern:** `emoji **Bold Title**\n\nDescription`

```python
# ✅ GOOD: Consistent callout pattern
_output = mo.callout(
    mo.md("👆 **Select a Project**\n\nClick a row in the table above to activate it."),
    kind="info",
)

# ❌ BAD: Plain text without styling
_output = mo.md("Select a project from the table above.")
```

**Emoji + Kind Reference:**

| Emoji | Kind | Usage |
|-------|------|-------|
| 👆 | `info` | Action required (Select, Configure, Click) |
| ⏳ | `info` | Waiting for dependency (data, chart loading) |
| ⚠️ | `warn` | Issue or missing prerequisite |
| ✅ | `success` | Completed action, confirmation |
| 🔴 | `danger` | Error or critical failure |

**Examples by state:**

```python
# Action required
mo.callout(mo.md("👆 **Load Data**\n\nEnter a file path and click Load."), kind="info")

# Waiting for dependency
mo.callout(mo.md("⏳ **Waiting for Chart**\n\nSelect a date range to load the chart."), kind="info")

# Warning
mo.callout(mo.md("⚠️ **Project Required**\n\nSelect a CDF project first."), kind="warn")

# Success
mo.callout(mo.md("✅ **Data Loaded**\n\n1,234 rows ready for analysis."), kind="success")

# Error
mo.callout(mo.md("🔴 **Connection Failed**\n\nCheck credentials and try again."), kind="danger")
```

### Reusable UI Factories

For consistent UX across multiple visualizations, create factory functions that return UI components. This ensures:
- Single source of truth for UI patterns
- Future improvements cascade to all instances
- Consistent user experience

**Example: Date Range Selector Factory**

```python
@app.cell(hide_code=True)
def define_date_range_helpers(datetime):
    """Reusable date range selector factory."""
    import re

    def create_date_range_ui(mo, min_date, max_date, title="Date Range", data_info=None):
        """
        Create a unified date range selector with dropdown presets and date_range picker.

        Args:
            mo: marimo module
            min_date: Minimum date in the data
            max_date: Maximum date in the data
            title: Section title
            data_info: Optional info string about data availability

        Returns:
            Tuple of (form, output_element)
        """
        date_form = mo.ui.batch(
            mo.md("""
**Quick select:** {preset}

**Date range:** {date_range}
            """),
            {
                "preset": mo.ui.dropdown(
                    options=[
                        "Last 7 days", "Last 14 days", "Last 30 days",
                        "Last 60 days", "Last 90 days", "Custom range", "All data",
                    ],
                    value="Last 7 days",
                ),
                "date_range": mo.ui.date_range(
                    start=min_date,
                    stop=max_date,
                    value=(max_date - datetime.timedelta(days=6), max_date),
                ),
            },
        ).form(submit_button_label="Apply")

        _info_text = f"_{data_info}_" if data_info else ""
        output = mo.vstack([
            mo.md(f"### {title}"),
            date_form,
            mo.md(_info_text) if _info_text else mo.md(""),
        ])
        return date_form, output

    def calculate_date_range_from_form(form_value, min_date, max_date):
        """Calculate effective start/end dates based on form submission."""
        # Default: last 7 days
        start_date = max_date - datetime.timedelta(days=6)
        end_date = max_date

        if form_value is not None:
            preset = form_value["preset"]
            date_range = form_value["date_range"]

            if preset == "All data":
                start_date, end_date = min_date, max_date
            elif preset == "Custom range":
                if date_range:
                    start_date, end_date = date_range
            else:
                # Parse "Last N days" format
                match = re.search(r"Last (\d+) days", preset)
                days = int(match.group(1)) if match else 7
                start_date = max_date - datetime.timedelta(days=days - 1)
                end_date = max_date

        return start_date, end_date

    return calculate_date_range_from_form, create_date_range_ui
```

**Usage in picker cells:**

```python
@app.cell(hide_code=True)
def create_my_date_picker(create_date_range_ui, datetime, jobs_df, mo):
    # Calculate data bounds
    _min_date = jobs_df["timestamp"].min().date()
    _max_date = jobs_df["timestamp"].max().date()
    _data_info = f"**Data available:** {_min_date} to {_max_date}"

    my_date_form, _output = create_date_range_ui(
        mo, _min_date, _max_date, title="Date Range for My Chart", data_info=_data_info
    )
    _output
    return (my_date_form,)
```

**Usage in calculation cells:**

```python
@app.cell(hide_code=True)
def extract_my_date_range(calculate_date_range_from_form, my_date_form, datetime, jobs_df):
    _min_date = jobs_df["timestamp"].min().date()
    _max_date = jobs_df["timestamp"].max().date()

    start_date, end_date = calculate_date_range_from_form(
        my_date_form.value, _min_date, _max_date
    )
    return end_date, start_date
```

**Benefits:**
- Adding new presets (e.g., "This month", "Year to date") only requires changing the factory
- All date pickers get the same UX improvements automatically
- Reduces code duplication across chapters

---

## 11. Testing in Marimo

### Static Analysis with `marimo check`

Use `marimo check` for fast static analysis before running tests. It catches issues like:
- **Variable dependency errors** - undefined references, cycles
- **Empty cells** - cells with only whitespace, comments, or `pass`
- **Markdown indentation** - style warnings for better readability
- **Multiple definitions** - same variable defined in multiple cells

```bash
# Run static analysis (fast, no dependencies needed)
uvx --python 3.13 marimo check marimo-tsjm-analysis.py

# Auto-fix fixable issues (e.g., markdown indentation)
uvx --python 3.13 marimo check --fix marimo-tsjm-analysis.py

# Example output:
# warning[empty-cells]: Empty cell can be removed
#  --> notebook.py:165:0
# Updated: notebook.py
# Found 1 issue.
```

**Tip:** Run `marimo check` as a pre-commit hook or in CI for early feedback.

See [marimo linting rules](https://docs.marimo.io/guides/lint_rules/) for full documentation.

### Running Tests with pytest

```bash
# With pyproject.toml
uv run pytest marimo-tsjm-analysis.py -v

# With inline dependencies (manual)
uvx --with marimo --with polars pytest notebook.py -v
```

### Test Variable Naming
Prefix test-local variables with `_` to avoid graph conflicts:

```python
@app.cell
def test_something(some_dependency):
    _result = some_dependency.compute()
    assert _result is not None
    _df = pl.DataFrame({"a": [1, 2, 3]})
    assert len(_df) == 3
    return
```

### Tests Catch Graph Errors
Pytest will catch marimo-specific errors that linters miss:
- `MultipleDefinitionError`
- `NameError` for undefined references
- Cycle dependencies

---

## 12. Naming Cell Functions

### Problem
By default, marimo creates cells with anonymous `def _()` functions. While this works, it makes debugging and tracing difficult:

```python
# ❌ BAD: Anonymous functions are hard to trace
@app.cell(hide_code=True)
def _():
    import marimo as mo
    mo.md("# Title")
    return (mo,)

@app.cell(hide_code=True)
def _(mo):
    mo.md("## Chapter 1")
    return

@app.cell(hide_code=True)
def _(jobs_df, pl):
    # Complex processing...
    return (events_df,)
```

Issues:
- **Debugging**: Stack traces show `_()` which doesn't help identify the failing cell
- **Tracing**: Marimo's UI trace feature shows `_` for all cells
- **Readability**: Hard to understand cell purpose at a glance

### Solution
Use descriptive function names that indicate the cell's purpose:

```python
# ✅ GOOD: Descriptive names for better debugging and tracing
@app.cell(hide_code=True)
def import_marimo_and_title():
    import marimo as mo
    mo.md("# Title")
    return (mo,)

@app.cell(hide_code=True)
def chapter1_setup_header(mo):
    mo.md("## Chapter 1")
    return

@app.cell(hide_code=True)
def calculate_concurrency_events(jobs_df, pl):
    # Complex processing...
    return (events_df,)
```

### Naming Conventions

Use consistent prefixes based on cell purpose:

| Pattern | Use For | Examples |
|---------|---------|----------|
| `chapterN_*_header` | Chapter/section headers | `chapter1_setup_header`, `chapter4_concurrency_header` |
| `import_*` | Import cells | `import_marimo_and_title`, `import_core_libraries` |
| `create_*` | UI element creation | `create_config_form`, `create_date_picker`, `create_metric_selector` |
| `extract_*` | Value extraction | `extract_config_values`, `extract_selected_dates` |
| `load_*` | Data loading | `load_env_config`, `load_jsonl_data` |
| `calculate_*` | Data processing | `calculate_events_df`, `calculate_metrics_date_range` |
| `show_*` | Display/output cells | `show_data_overview`, `show_active_jobs_details` |
| `test_*` | Test functions | `test_concurrency_calculation`, `test_json_extraction` |

### Benefits

1. **Better Debugging**: Stack traces show meaningful function names
   ```
   Cell calculate_concurrency_events, line 45
   ```
   vs
   ```
   Cell _, line 45
   ```

2. **Improved Tracing**: Marimo's UI trace shows the actual function name, making it easier to understand the execution flow

3. **Code Navigation**: IDE features like "Go to Definition" work better with named functions

4. **Self-Documentation**: The notebook structure is clear from function names alone

### Example: Full Chapter Organization

```python
# Chapter 4: Concurrency Analysis
@app.cell(hide_code=True)
def chapter4_concurrency_header(mo):
    mo.md("## Chapter 4: Concurrency Analysis")
    return

@app.cell(hide_code=True)
def create_concurrency_date_picker(datetime, jobs_df, mo):
    # Date range picker UI
    return (date_range_picker,)

@app.cell(hide_code=True)
def extract_concurrency_date_range(date_range_picker):
    # Extract selected dates
    return selected_start, selected_end

@app.cell(hide_code=True)
def calculate_concurrency_events(jobs_df, pl):
    # Calculate events and concurrency
    return (events_df,)

@app.cell(hide_code=True)
def create_concurrency_chart(alt, events_df, mo, pl):
    # Build and display the chart
    return (chart_element,)

@app.cell(hide_code=True)
def show_active_jobs_details(chart_element, jobs_df, mo, pl):
    # Show details when chart is selected
    return
```

---

## 13. Polars Schema Inference Issues

### Problem
```
ComputeError: got non-null value for NULL-typed column: UNAVAILABLE: ...
```

This happens when:
1. Early rows have `null` for a column
2. Polars infers the column as `Null` type
3. Later rows have actual values

### Solution
Provide `schema_overrides` for columns that may have nulls in early rows:

```python
_schema_overrides = {
    "tsj_error": pl.Utf8,        # Can be null or error string
    "tsjm_last_counts": pl.Utf8, # JSON string, can be "{}" or actual data
    "tsj_last_seen_time": pl.Int64,  # Can be null timestamp
}
jobs_df = pl.read_ndjson(file_path, schema_overrides=_schema_overrides)
```

### Common Problematic Columns
- Error message columns (often null for successful jobs)
- Optional metadata fields
- Nullable timestamps

---

## Quick Reference

| Issue | Solution |
|-------|----------|
| Conditional output not rendering | Use `_output` variable, display at end |
| Polars NULL-typed column error | Use `schema_overrides` in `read_ndjson()` |
| Unwanted re-runs on input | Use `.form()` to gate with submit |
| Multiple definition error | Prefix locals with `_` |
| Progress bar shows only at end | Pass iterator directly, not `list()` |
| Output after progress bar | Use `mo.output.append()` |
| Chart selection causes error | Convert to pandas before charting |
| Datetime filter mismatch | Cast to microseconds (`us`) |
| Early return needed | Use if/else, single `return` at end (no mid-cell returns) |
| Silent exception | Track errors, show count to user |
| Search in dropdown | `searchable=True` |
| Hard to debug/trace cells | Use descriptive function names instead of `def _()` |
| Duplicate UI patterns | Create reusable factory functions |
| Date range needed in multiple charts | Use shared `create_date_range_ui` factory |
| Click + brush selection conflicts | Use only `selection_interval` (brush), avoid `selection_point` |
| Hover snapping triggers updates | Use native tooltips (hover over point), no snapping |
| Debug info in app mode | Check `mo.app_meta().mode == "edit"` |

---

## References

- [Marimo Interactive Elements](https://docs.marimo.io/guides/interactivity/)
- [Marimo Composite Elements](https://docs.marimo.io/guides/interactivity/#composite-elements)
- [Marimo Testing with pytest](https://docs.marimo.io/guides/testing/pytest/)
- [Altair Selection Documentation](https://altair-viz.github.io/user_guide/interactions.html)

