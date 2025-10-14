"""
IRP Notebook Framework - Display Utilities
"""

import pandas as pd
from typing import List, Dict, Any
from helpers.constants import DISPLAY_CONFIG

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

def header(title: str, width: int = 60):
    _display_html(f"<h3 style='font-weight:bold; font-size:1.1em; color:#111;'>{title}</h3>")

def subheader(title: str, width: int = 60):
    _display_html(f"<h4 style='font-weight:bold; font-size:1.0em; color:#333;'>{title}</h4>")

def success(message: str):
    _display_html(f"<span style='color:green; font-weight:bold;'>{message}</span>")

def error(message: str):
    _display_html(f"<span style='color:red; font-weight:bold;'>{message}</span>")

def warning(message: str):
    _display_html(f"<span style='color:#b58900; font-weight:bold;'>{message}</span>")

def info(message: str):
    _display_html(f"<span style='color:#005f9e;'>{message}</span>")

# ---------------------------------------------------------------------
# Tables and DataFrames
# ---------------------------------------------------------------------

def table(data: List[List], headers: List[str], title: str = None, tablefmt: str = "grid"):
    if title:
        subheader(title)
    df = pd.DataFrame(data, columns=headers)
    _display_html(df.to_html(index=False))

def dataframe(df: pd.DataFrame, title: str = None, max_rows: int = None, max_cols: int = None):
    if title:
        subheader(title)
    if df is None or df.empty:
        info("(No data)")
        return
    pd.set_option('display.max_rows', max_rows or DISPLAY_CONFIG.get('max_table_rows', 1000))
    pd.set_option('display.max_columns', max_cols)
    _display_html(df.to_html(index=False))

# ---------------------------------------------------------------------
# Progress / Simple summaries
# ---------------------------------------------------------------------

def pgbar(current: int, total: int, title: str = "", width: int = None):
    percent = (current / total * 100) if total else 0
    _display_html(f"{title}: <b>{percent:.1f}%</b> ({current}/{total})")

def cycle_summary(cycle: Dict[str, Any]):
    header(f"Cycle: {cycle.get('cycle_name', 'Unknown')}")
    data = {
        "Status": cycle.get("status", "N/A"),
        "Created": str(cycle.get("created_ts", "N/A")),
        "Created By": cycle.get("created_by", "N/A")
    }
    if cycle.get("archived_ts"):
        data["Archived"] = str(cycle["archived_ts"])
    df = pd.DataFrame(list(data.items()), columns=["Property", "Value"])
    _display_html(df.to_html(index=False))

def step_progress(progress_df: pd.DataFrame):
    if progress_df is None or progress_df.empty:
        info("No steps found")
        return
    current_stage = None
    for _, row in progress_df.iterrows():
        stage = row.get("stage_name")
        if stage != current_stage:
            subheader(f"Stage: {stage}")
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
        subheader(title)
    df = pd.DataFrame(list(data.items()), columns=["Key", "Value"])
    _display_html(df.to_html(index=False))

def json(data: Dict[str, Any], title: str = None, indent: int = 2):
    import json
    if title:
        subheader(title)
    _display_html(f"<pre>{json.dumps(data, indent=indent, default=str)}</pre>")

def section(title: str, content: str = None):
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

#---------------------------------------------------------------------
# Blocking prompts for user input
#---------------------------------------------------------------------
def yes_no(prompt="Continue execution?"):
    """
    Simple yes/no prompt that blocks execution until user responds.
    Returns True for yes, False for no.
    """
    while True:
        response = input(f"{prompt} (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        print("Please enter 'y' or 'n'")

def _default_validation(text):
    """Default validation function that checks if input is not blank"""
    return bool(text.strip())

def text_input(prompt="Enter value:", default="", placeholder="", validation=None):
    """
    Simple text input that blocks execution until user submits valid input.
    
    Args:
        prompt: Text prompt to show to the user
        default: Default value to suggest
        placeholder: Example text to show in the prompt as a hint
        validation: Optional validation function that returns True/False
                   Defaults to non-blank validation if None
    
    Returns:
        str: The validated input text, or None if cancelled
    """
    # Use default validation if none provided
    if validation is None:
        validation = _default_validation
    
    # Build complete display prompt
    display_prompt = prompt
    
    # Add placeholder if provided
    if placeholder:
        display_prompt = f"{display_prompt} (e.g., {placeholder})"
    
    # Add default if provided
    if default:
        display_prompt = f"{display_prompt} [default: {default}]"
    
    # Print the complete prompt
    print(f"{display_prompt} (enter 'cancel' to stop)")
    
    while True:
        response = input("> ").strip()
        
        # Check for cancel
        if response.lower() == 'cancel':
            return None
        
        # Use default if empty response and default exists
        if not response and default:
            response = default
        
        # Validate the input
        if validation(response):
            return response
        else:
            print("Invalid input. Please try again or enter 'cancel' to stop.")

def dropdown(options, prompt="Select an option:", default=None):
    """
    Simple menu selection that blocks execution until user chooses.
    Returns the selected option, or None if cancelled.
    """
    if not options:
        return None
    
    default_option = options[default] if default and 0 <= default < len(options) else None
    display_prompt = prompt
    if default_option:
        display_prompt = f"{prompt} [default: {default_option}]"
    
    print(f"{display_prompt}")
    print("Options:")
    for i, option in enumerate(options):
        print(f"{i+1}. {option}")
    print(f"Enter 1-{len(options)} or 'cancel' to stop")
    
    while True:
        response = input("> ").strip()
        
        # Check for cancel
        if response.lower() == 'cancel':
            return None
            
        # Use default if empty and default exists
        if not response and default_option:
            return default_option
        
        try:
            idx = int(response) - 1
            if 0 <= idx < len(options):
                return options[idx]
        except ValueError:
            pass
        print(f"Please enter a number between 1 and {len(options)}")