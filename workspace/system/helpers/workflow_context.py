import os
import re
from dataclasses import dataclass

@dataclass
class WorkflowContext:
    cycle_name: str
    stage_num: int
    stage_name: str
    step_num: int
    step_name: str
    notebook_path: str

def get_workflow_context(cwd: str = None) -> WorkflowContext:
    """Parse the current working directory and notebook name into a WorkflowContext."""

    if cwd is None:
        cwd = os.getcwd()

    # Extract cycle name (between Active_ and /notebooks/)
    cycle_pattern = r"(?<=/workspace/workflows/Active_)([^/]+)(?=/notebooks/)"
    cycle_match = re.search(cycle_pattern, cwd)
    if not cycle_match:
        raise ValueError(f"Could not parse cycle name from cwd: {cwd}")
    cycle_name = cycle_match.group(1)

    # Extract stage (Stage_01_Setup)
    stage_dir = os.path.basename(cwd)
    stage_pattern = r"Stage_(\d+)_(.+)"
    stage_match = re.match(stage_pattern, stage_dir)
    if not stage_match:
        raise ValueError(f"Could not parse stage from directory: {stage_dir}")
    stage_num = int(stage_match.group(1))
    stage_name = stage_match.group(2)

    # Find the active notebook (assumes .ipynb file in this directory)
    notebook_file = None
    for f in os.listdir(cwd):
        if f.endswith(".ipynb") and f.startswith("Step_"):
            notebook_file = f
            break
    if not notebook_file:
        raise FileNotFoundError(f"Not a step notebook in {cwd}")

    # Extract step (Step_01_Initialize.ipynb)
    step_pattern = r"Step_(\d+)_(.+)\.ipynb"
    step_match = re.match(step_pattern, notebook_file)
    if not step_match:
        raise ValueError(f"Could not parse step from file: {notebook_file}")
    step_num = int(step_match.group(1))
    step_name = step_match.group(2)

    return WorkflowContext(
        cycle_name=cycle_name,
        stage_num=stage_num,
        stage_name=stage_name,
        step_num=step_num,
        step_name=step_name,
        notebook_path=os.path.join(cwd, notebook_file),
    )
