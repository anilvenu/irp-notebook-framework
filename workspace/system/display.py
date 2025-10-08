"""
IRP Notebook Framework - Display Utilities
"""

import pandas as pd
from typing import List, Dict, Any
from .config import DISPLAY_CONFIG

class NotebookCanceled(Exception):
    """Raised when user clicks Cancel in a blocking prompt."""
    pass

def _in_ipython():
    try:
        from IPython import get_ipython
        return get_ipython() is not None
    except Exception:
        return False

def _display_html(html: str):
    if _in_ipython():
        from IPython.display import display, HTML
        display(HTML(html))
    else:
        import re
        print(re.sub("<[^<]+?>", "", html))

# ---------------------------------------------------------------------
# Basic prints (with some HTML formatting)
# ---------------------------------------------------------------------

def print_header(title: str, width: int = 60):
    _display_html(f"<h3 style='font-weight:bold; font-size:1.1em; color:#111;'>{title}</h3>")

def print_subheader(title: str, width: int = 60):
    _display_html(f"<h4 style='font-weight:bold; font-size:1.0em; color:#333;'>{title}</h4>")

def print_success(message: str):
    _display_html(f"<span style='color:green; font-weight:bold;'>{message}</span>")

def print_error(message: str):
    _display_html(f"<span style='color:red; font-weight:bold;'>{message}</span>")

def print_warning(message: str):
    _display_html(f"<span style='color:#b58900; font-weight:bold;'>{message}</span>")

def print_info(message: str):
    _display_html(f"<span style='color:#005f9e;'>{message}</span>")

# ---------------------------------------------------------------------
# Tables and DataFrames
# ---------------------------------------------------------------------

def display_table(data: List[List], headers: List[str], title: str = None, tablefmt: str = "grid"):
    if title:
        print_subheader(title)
    df = pd.DataFrame(data, columns=headers)
    _display_html(df.to_html(index=False))

def display_dataframe(df: pd.DataFrame, title: str = None, max_rows: int = None, max_cols: int = None):
    if title:
        print_subheader(title)
    if df is None or df.empty:
        print_info("(No data)")
        return
    pd.set_option('display.max_rows', max_rows or DISPLAY_CONFIG.get('max_table_rows', 1000))
    pd.set_option('display.max_columns', max_cols)
    _display_html(df.to_html(index=False))

# ---------------------------------------------------------------------
# Progress / Simple summaries
# ---------------------------------------------------------------------

def display_progress_bar(current: int, total: int, title: str = "", width: int = None):
    percent = (current / total * 100) if total else 0
    _display_html(f"{title}: <b>{percent:.1f}%</b> ({current}/{total})")

def display_cycle_summary(cycle: Dict[str, Any]):
    print_header(f"Cycle: {cycle.get('cycle_name', 'Unknown')}")
    data = {
        "Status": cycle.get("status", "N/A"),
        "Created": str(cycle.get("created_ts", "N/A")),
        "Created By": cycle.get("created_by", "N/A")
    }
    if cycle.get("archived_ts"):
        data["Archived"] = str(cycle["archived_ts"])
    df = pd.DataFrame(list(data.items()), columns=["Property", "Value"])
    _display_html(df.to_html(index=False))

def display_step_progress(progress_df: pd.DataFrame):
    if progress_df is None or progress_df.empty:
        print_info("No steps found")
        return
    current_stage = None
    for _, row in progress_df.iterrows():
        stage = row.get("stage_name")
        if stage != current_stage:
            print_subheader(f"Stage: {stage}")
            current_stage = stage
        step = row.get("step_name", "")
        status = row.get("last_status", "")
        _display_html(f"<i>{step}</i>: <b>{status}</b>")

# ---------------------------------------------------------------------
# Simple formatting helpers
# ---------------------------------------------------------------------

def format_duration(seconds: float) -> str:
    if not seconds:
        return "N/A"
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    return f"{seconds/3600:.1f}h"

def format_timestamp(ts) -> str:
    if ts is None:
        return "N/A"
    if hasattr(ts, "strftime"):
        return ts.strftime(DISPLAY_CONFIG.get("date_format", "%Y-%m-%d %H:%M"))
    return str(ts)

def display_key_value(data: Dict[str, Any], title: str = None):
    if title:
        print_subheader(title)
    df = pd.DataFrame(list(data.items()), columns=["Key", "Value"])
    _display_html(df.to_html(index=False))

def display_json(data: Dict[str, Any], title: str = None, indent: int = 2):
    import json
    if title:
        print_subheader(title)
    _display_html(f"<pre>{json.dumps(data, indent=indent, default=str)}</pre>")

def display_section(title: str, content: str = None):
    _display_html(f"<h4>{title}</h4>")
    if content:
        _display_html(f"<p>{content}</p>")

def clear_output():
    try:
        if _in_ipython():
            from IPython.display import clear_output as jupyter_clear
            jupyter_clear(wait=True)
        else:
            print("\n" * 3)
    except Exception:
        print("\n" * 3)

