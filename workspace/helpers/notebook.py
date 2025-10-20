"""
Helper functions for Jupyter notebook operations.

These functions facilitate interactions with Jupyter notebooks,
such as retrieving the current notebook's file path. They must be
used within a Jupyter environment.
"""
from IPython import get_ipython
import os
import json

def get_current_notebook_path() -> str:
    """
    Get the full path of the currently running Jupyter notebook (.ipynb).
    Works by inspecting IPython's user namespace (__session__ variable).
    """
    try:
        ipython = get_ipython()
        if ipython is None:
            raise RuntimeError("Not running inside an IPython/Jupyter environment")

        # The __session__ variable usually contains the full notebook path
        nb_path = ipython.user_ns.get("__session__")
        if not nb_path:
            raise RuntimeError("Notebook path not found in IPython user_ns")

        # Normalize path (e.g., ensure it’s absolute)
        nb_path = os.path.abspath(nb_path)
        return nb_path

    except Exception as e:
        raise RuntimeError(f"Failed to detect notebook path: {e}")
    
def extract_top_markdown(notebook_path):
    """Extract markdown cells from the top of a notebook until a non-markdown cell is found."""
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        
        markdown_content = []
        for cell in nb.get('cells', []):
            if cell.get('cell_type') == 'markdown':
                # Join the source lines into a single string
                source = cell.get('source', [])
                if isinstance(source, list):
                    markdown_content.append(''.join(source))
                else:
                    markdown_content.append(source)
            else:
                # Stop when we hit a non-markdown cell
                break
        
        return '\n\n'.join(markdown_content) if markdown_content else ''
    except Exception as e:
        return f"Error reading {notebook_path}: {str(e)}"