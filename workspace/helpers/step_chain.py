"""
Step chaining configuration and execution logic.

This module defines the relationships between steps in a workflow stage,
enabling automatic execution of subsequent notebooks when batches complete.
"""

from typing import Optional, Dict, Any, List
from pathlib import Path
import logging

from helpers.database import execute_query
from helpers.constants import BatchStatus, CycleStatus

logger = logging.getLogger(__name__)


# Stage 02 Step Chain Configuration
# Data Extraction → Control Totals chaining
STAGE_02_CHAIN = {
    1: {
        'next_step': 2,
        'batch_type': 'Data Extraction',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'Execute Data Extraction → Control Totals'
    },
    2: {
        'next_step': None,  # Final step in Stage 02
        'batch_type': None,
        'wait_for': None,
        'description': 'Control Totals (Final Step)'
    }
}

# Stage 03 Step Chain Configuration
# Maps step numbers to their next step and execution conditions
STAGE_03_CHAIN = {
    1: {
        'next_step': 2,
        'batch_type': 'EDM Creation',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'EDM Creation → Portfolio Creation'
    },
    2: {
        'next_step': 3,
        'batch_type': 'Portfolio Creation',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'Portfolio Creation → MRI Import'
    },
    3: {
        'next_step': 4,
        'batch_type': 'MRI Import',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'MRI Import → Create Reinsurance Treaties'
    },
    4: {
        'next_step': 5,
        'batch_type': 'Create Reinsurance Treaties',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'Create Reinsurance Treaties → EDM DB Upgrade'
    },
    5: {
        'next_step': 6,
        'batch_type': 'EDM DB Upgrade',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'EDM DB Upgrade → GeoHaz'
    },
    6: {
        'next_step': 7,
        'batch_type': 'GeoHaz',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'GeoHaz → Portfolio Mapping'
    },
    7: {
        'next_step': 8,
        'batch_type': 'Portfolio Mapping',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'Portfolio Mapping → Control Totals'
    },
    8: {
        'next_step': None,  # Final step
        'batch_type': None,
        'wait_for': None,
        'description': 'Control Totals (Final Step)'
    }
}

# Stage 04 Step Chain Configuration
# Maps step numbers to their next step and execution conditions
# Note: wait_for can be a single status or a list of statuses
STAGE_04_CHAIN = {
    1: {
        'next_step': 2,
        'batch_type': 'Analysis',
        'wait_for': [BatchStatus.COMPLETED, BatchStatus.FAILED],  # Chain on success OR failure
        'description': 'Execute Analysis → Analysis Summary'
    },
    2: {
        'next_step': None,  # Final step in Stage 04
        'batch_type': None,
        'wait_for': None,
        'description': 'Analysis Summary (Final Step)'
    }
}

# Stage 05 Step Chain Configuration
# Grouping → Grouping Rollup → Grouping Summary chaining
# Rollup groups (groups of groups) can only be created AFTER base groups exist
STAGE_05_CHAIN = {
    1: {
        'next_step': 2,
        'batch_type': 'Grouping',
        'wait_for': BatchStatus.COMPLETED,
        'description': 'Group Analysis Results → Group Rollup'
    },
    2: {
        'next_step': 3,
        'batch_type': 'Grouping Rollup',
        'wait_for': [BatchStatus.COMPLETED, BatchStatus.FAILED],  # Chain on success OR failure
        'description': 'Group Rollup → Grouping Summary'
    },
    3: {
        'next_step': None,  # Final step in Stage 05
        'batch_type': None,
        'wait_for': None,
        'description': 'Grouping Summary (Final Step)'
    }
}

# Stage 06 Step Chain Configuration
# Export to RDM → Verify RDM Export chaining
STAGE_06_CHAIN = {
    1: {
        'next_step': 2,
        'batch_type': 'Export to RDM',
        'wait_for': [BatchStatus.COMPLETED, BatchStatus.FAILED],  # Chain on success OR failure
        'description': 'Export to RDM → Verify RDM Export'
    },
    2: {
        'next_step': None,  # Final step in Stage 06
        'batch_type': None,
        'wait_for': None,
        'description': 'Verify RDM Export (Final Step)'
    }
}

# Combined chain configuration by stage
STAGE_CHAINS = {
    2: STAGE_02_CHAIN,
    3: STAGE_03_CHAIN,
    4: STAGE_04_CHAIN,
    5: STAGE_05_CHAIN,
    6: STAGE_06_CHAIN,
}


def get_next_step_info(batch_id: int, schema: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get information about the next step to execute after a batch completes.

    Args:
        batch_id: The ID of the completed batch
        schema: Database schema to use

    Returns:
        Dictionary containing next step information:
        {
            'cycle_name': str,
            'stage_num': int,
            'step_num': int,
            'notebook_path': Path,
            'current_step_num': int
        }
        Returns None if no next step exists or conditions not met.
    """
    # Query batch and step information
    query = """
        SELECT
            b.batch_type,
            b.status as batch_status,
            c.cycle_name,
            st.stage_num,
            s.step_num,
            c.status as cycle_status
        FROM irp_batch b
        JOIN irp_step_run sr ON b.step_id = sr.step_id
        JOIN irp_step s ON sr.step_id = s.id
        JOIN irp_stage st ON s.stage_id = st.id
        JOIN irp_cycle c ON st.cycle_id = c.id
        WHERE b.id = %s
    """

    result = execute_query(query, (batch_id,), schema=schema)

    if result.empty:
        logger.warning(f"Batch {batch_id} not found")
        return None

    row = result.iloc[0]
    batch_type = row['batch_type']
    batch_status = row['batch_status']
    cycle_name = row['cycle_name']
    stage_num = row['stage_num']
    step_num = row['step_num']
    cycle_status = row['cycle_status']

    # Validate cycle is still active
    if cycle_status != CycleStatus.ACTIVE:
        logger.info(f"Cycle {cycle_name} is not ACTIVE (status={cycle_status}), skipping chain")
        return None

    # Check if stage has chain configuration
    if stage_num not in STAGE_CHAINS:
        logger.debug(f"Step chaining not configured for Stage {stage_num}")
        return None

    stage_chain = STAGE_CHAINS[stage_num]

    # Check if current step has a next step defined
    if step_num not in stage_chain:
        logger.warning(f"Step {step_num} not found in Stage {stage_num} chain configuration")
        return None

    chain_config = stage_chain[step_num]

    # Validate batch type matches expected
    if chain_config['batch_type'] != batch_type:
        logger.warning(
            f"Batch type mismatch: expected {chain_config['batch_type']}, got {batch_type}"
        )
        return None

    # Check if batch reached required status
    # wait_for can be a single status or a list of statuses
    wait_for = chain_config['wait_for']
    if isinstance(wait_for, list):
        status_match = batch_status in wait_for
    else:
        status_match = batch_status == wait_for

    if not status_match:
        logger.debug(
            f"Batch {batch_id} status {batch_status} does not match required "
            f"{wait_for}, skipping chain"
        )
        return None

    # Check if there is a next step
    next_step_num = chain_config['next_step']
    if next_step_num is None:
        logger.info(f"Step {step_num} is the final step in chain, no next step to execute")
        return None

    # Build next step notebook path
    notebook_path = _build_notebook_path(cycle_name, stage_num, next_step_num)

    return {
        'cycle_name': cycle_name,
        'stage_num': stage_num,
        'step_num': next_step_num,
        'notebook_path': notebook_path,
        'current_step_num': step_num,
        'description': chain_config['description']
    }


def should_execute_next_step(
    batch_id: int,
    schema: Optional[str] = None
) -> bool:
    """
    Determine if the next step should be automatically executed.

    This checks:
    1. Next step exists in chain configuration
    2. Cycle is still ACTIVE
    3. Batch reached required status

    Note: We intentionally do NOT check if the next step was already executed.
    This allows re-running workflows after entity deletion - submit_batch will
    check entity existence and only resubmit jobs for missing entities.

    Args:
        batch_id: The ID of the completed batch
        schema: Database schema to use

    Returns:
        True if next step should be executed, False otherwise
    """
    next_step_info = get_next_step_info(batch_id, schema=schema)

    if next_step_info is None:
        return False

    logger.info(
        f"Batch {batch_id} completed: {next_step_info['description']} - "
        f"ready to execute Step {next_step_info['step_num']}"
    )
    return True


def _build_notebook_path(cycle_name: str, stage_num: int, step_num: int) -> Path:
    """
    Build the path to a notebook file.

    Args:
        cycle_name: Name of the cycle (e.g., "Analysis-2025-Q1")
        stage_num: Stage number (1-based)
        step_num: Step number (1-based)

    Returns:
        Path object pointing to the notebook file
    """
    # Map step numbers to notebook filenames by stage
    STAGE_NOTEBOOKS = {
        2: {
            1: 'Step_01_Execute_Data_Extraction.ipynb',
            2: 'Step_02_Control_Totals.ipynb'
        },
        3: {
            1: 'Submit_Create_EDM_Batch.ipynb',
            2: 'Create_Base_Portfolios.ipynb',
            3: 'Submit_MRI_Import_Batch.ipynb',
            4: 'Create_Reinsurance_Treaties.ipynb',
            5: 'Submit_EDM_Version_Upgrade_Batch.ipynb',
            6: 'Submit_GeoHaz_Batch.ipynb',
            7: 'Execute_Portfolio_Mapping.ipynb',
            8: 'Control_Totals.ipynb'
        },
        4: {
            1: 'Execute_Analysis.ipynb',
            2: 'Analysis_Summary.ipynb'
        },
        5: {
            1: 'Group_Analysis_Results.ipynb',
            2: 'Group_Rollup.ipynb',
            3: 'Grouping_Summary.ipynb'
        },
        6: {
            1: 'Export_to_RDM.ipynb',
            2: 'Verify_RDM_Export.ipynb'
        }
    }

    if stage_num not in STAGE_NOTEBOOKS:
        raise ValueError(f"No notebooks configured for Stage {stage_num}")

    stage_notebooks = STAGE_NOTEBOOKS[stage_num]
    notebook_filename = stage_notebooks.get(step_num)
    if notebook_filename is None:
        raise ValueError(f"No notebook configured for Stage {stage_num} Step {step_num}")

    # Build path: workflows/Active_{cycle}/notebooks/Stage_{stage:02d}_*/Step_{step:02d}_{notebook}
    # We'll need to find the actual stage directory name
    workflows_dir = Path(__file__).parent.parent / 'workflows'
    cycle_dir = workflows_dir / f'Active_{cycle_name}'

    # Find stage directory (has dynamic suffix like "Stage_03_Data_Import")
    stage_pattern = f'Stage_{stage_num:02d}_*'
    stage_dirs = list((cycle_dir / 'notebooks').glob(stage_pattern))

    if not stage_dirs:
        raise FileNotFoundError(
            f"Stage directory not found: {cycle_dir / 'notebooks' / stage_pattern}"
        )

    stage_dir = stage_dirs[0]  # Should only be one match

    # Find step notebook
    step_pattern = f'Step_{step_num:02d}_*{notebook_filename}'
    step_files = list(stage_dir.glob(step_pattern))

    if not step_files:
        # Try without step prefix pattern (direct filename)
        notebook_path = stage_dir / notebook_filename
        if not notebook_path.exists():
            raise FileNotFoundError(
                f"Step notebook not found: {stage_dir / step_pattern}"
            )
        return notebook_path

    return step_files[0]


def get_chain_status(cycle_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get the execution status of all steps in Stage 03 chain.

    Args:
        cycle_name: Name of the cycle
        schema: Database schema to use

    Returns:
        List of dictionaries with step status information
    """
    query = """
        SELECT
            st.stage_num,
            s.step_num,
            sr.status as step_status,
            sr.started_ts,
            sr.completed_ts,
            b.batch_type,
            b.status as batch_status,
            b.completed_ts as batch_completed_ts
        FROM irp_cycle c
        JOIN irp_stage st ON c.id = st.cycle_id
        JOIN irp_step s ON st.id = s.stage_id
        JOIN irp_step_run sr ON s.id = sr.step_id
        LEFT JOIN irp_batch b ON b.step_id = sr.id
        WHERE c.cycle_name = %s
          AND st.stage_num = 3
        ORDER BY s.step_num
    """

    result = execute_query(query, (cycle_name,), schema=schema)

    chain_status = []
    for step_num in range(1, 9):  # Steps 1-8
        step_data = result[result['step_num'] == step_num]

        if step_data.empty:
            status = {
                'step_num': step_num,
                'step_status': 'NOT_STARTED',
                'batch_status': None,
                'config': STAGE_03_CHAIN.get(step_num, {})
            }
        else:
            row = step_data.iloc[0]
            status = {
                'step_num': step_num,
                'step_status': row['step_status'],
                'batch_type': row.get('batch_type'),
                'batch_status': row.get('batch_status'),
                'started_ts': row['started_ts'],
                'completed_ts': row['completed_ts'],
                'batch_completed_ts': row.get('batch_completed_ts'),
                'config': STAGE_03_CHAIN.get(step_num, {})
            }

        chain_status.append(status)

    return chain_status
