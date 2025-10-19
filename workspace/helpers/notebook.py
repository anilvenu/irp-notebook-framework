"""
Helper functions for Jupyter notebook operations.

These functions facilitate interactions with Jupyter notebooks,
such as retrieving the current notebook's file path. They must be
used within a Jupyter environment.
"""
from IPython import get_ipython
import os


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