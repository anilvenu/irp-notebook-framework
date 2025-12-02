"""
IRP Notebook Framework - Step Operations

This module provides CRUD operations for steps and step runs, and manages
step execution lifecycle through the Step class.

ARCHITECTURE:
-------------
Layer 2 (CRUD): get_or_create_step, get_step_info, get_last_step_run,
                create_step_run, update_step_run
Layer 3 (Workflow): Step class (manages step execution lifecycle)

TRANSACTION BEHAVIOR:
--------------------
- All CRUD functions (Layer 2) never manage transactions
- They are safe to call within or outside transaction_context()
- The Step class (Layer 3) uses individual operations (no multi-operation transactions)
"""

from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from .context import WorkContext
from .database import execute_query, execute_insert, execute_scalar, execute_command
from .constants import StepStatus, SYSTEM_USER


class StepError(Exception):
    """Custom exception for step execution errors"""
    pass


# ============================================================================
# STEP CRUD OPERATIONS (Layer 2)
# ============================================================================

def get_or_create_step(
    stage_id: int,
    step_num: int,
    step_name: str,
    notebook_path: str = None,
) -> int:
    """
    Get existing step or create new one.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        stage_id: ID of the parent stage
        step_num: Step number within the stage
        step_name: Human-readable name for the step
        notebook_path: Path to associated notebook (optional)

    Returns:
        Step ID (existing or newly created)
    """
    # Try to get existing
    query = "SELECT id FROM irp_step WHERE stage_id = %s AND step_num = %s"
    step_id = execute_scalar(query, (stage_id, step_num))

    if step_id:
        return step_id

    # Create new
    query = """
        INSERT INTO irp_step (stage_id, step_num, step_name, notebook_path)
        VALUES (%s, %s, %s, %s)
    """
    return execute_insert(query, (stage_id, step_num, step_name, notebook_path))


def get_step_info(step_id: int) -> Optional[Dict[str, Any]]:
    """
    Get step information.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        step_id: ID of the step

    Returns:
        Dictionary with step information or None if not found
    """
    query = """
        SELECT
            st.id, st.step_num, st.step_name, st.notebook_path,
            sg.stage_num, sg.stage_name,
            c.cycle_name, c.id as cycle_id
        FROM irp_step st
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        WHERE st.id = %s
    """
    df = execute_query(query, (step_id,))
    return df.iloc[0].to_dict() if not df.empty else None


# ============================================================================
# STEP RUN CRUD OPERATIONS (Layer 2)
# ============================================================================

def get_last_step_run(step_id: int) -> Optional[Dict[str, Any]]:
    """
    Get the most recent run for a step.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        step_id: ID of the step

    Returns:
        Dictionary with step run information or None if not found
    """
    query = """
        SELECT id, step_id, run_num, status, started_ts, completed_ts,
               started_by, error_message, output_data
        FROM irp_step_run
        WHERE step_id = %s
        ORDER BY run_num DESC
        LIMIT 1
    """
    df = execute_query(query, (step_id,))
    return df.iloc[0].to_dict() if not df.empty else None


def create_step_run(step_id: int, started_by: str) -> Tuple[int, int]:
    """
    Create new step run.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        step_id: ID of the step
        started_by: User who started the run

    Returns:
        Tuple of (run_id, run_num)
    """
    # Get next run number
    query = "SELECT COALESCE(MAX(run_num), 0) + 1 FROM irp_step_run WHERE step_id = %s"
    run_num = execute_scalar(query, (step_id,))

    # Create run
    query = """
        INSERT INTO irp_step_run (step_id, run_num, status, started_by)
        VALUES (%s, %s, 'ACTIVE', %s)
    """
    run_id = execute_insert(query, (step_id, run_num, started_by))

    return run_id, run_num


def update_step_run(
    run_id: int,
    status: str,
    error_message: str = None,
    output_data: Dict = None
) -> bool:
    """
    Update step run with completion status.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        run_id: ID of the step run
        status: New status (ACTIVE, COMPLETED, FAILED, SKIPPED)
        error_message: Error message if failed (optional)
        output_data: Output data from step execution (optional)

    Returns:
        True if updated successfully, False otherwise
    """
    import json

    query = """
        UPDATE irp_step_run
        SET status = %s,
            completed_ts = CASE WHEN %s IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN NOW() ELSE completed_ts END,
            error_message = %s,
            output_data = %s
        WHERE id = %s
    """

    rows = execute_command(query, (
        status,
        status,
        error_message,
        json.dumps(output_data) if output_data else None,
        run_id
    ))

    return rows > 0


# ============================================================================
# STEP EXECUTION TRACKER (Layer 3)
# ============================================================================

class Step:
    """
    Manages step execution lifecycle and tracking.
    
    Usage:
        # Create context
        context = WorkContext()
        
        # Initialize step
        step = Step(context)
        
        # Check if previously executed
        if step.executed:
            raise Exception(step.status_message)
        
        # During execution
        step.log("Processing data...")
        step.checkpoint({"records": 100})
        
        # Complete
        step.complete({"total_records": 500})
        
        # Or fail
        step.fail("Error message")
    """
    
    def __init__(
        self,
        context: WorkContext
    ):
        """
        Initialize step tracker.
        
        Args:
            context: WorkContext object with cycle/stage/step info
        """
        
        self.context = context
        self.step_id = context.step_id

        # Get step info
        self.step_info = get_step_info(self.step_id)

        # Check execution eligibility
        self.executed = False
        self.status_message = ""
        self._executed()
        
        # Initialize run tracking
        self.run_id = None
        self.run_num = None
        self.logs = []
        
        # Auto-start if not executed
        if not self.executed:
            self.start()
    
    
    def _executed(self):
        """Check if step can be executed based on current state"""

        # Get last run
        last_run = get_last_step_run(self.step_id)
        
        if not last_run:
            # Never run before - OK to execute
            self.executed = False
            self.status_message = "Step has not been run yet"
            return
        
        status = last_run['status']
        
        self.executed = True
        self.status_message = f"Step already run, in status: {status}"
    
    
    def start(self, force: bool = False):
        """Start a new step run"""
        
        if self.executed:
            if force:
                print("Warning: Forcing re-execution of already executed step")
            else:
                raise StepError(f"Cannot execute step: {self.status_message}")
        
        try:
            # Create new step run
            self.run_id, self.run_num = create_step_run(
                self.step_id,
                SYSTEM_USER
            )
            
            self.start_time = datetime.now()
            
            print(f"Starting Step Run #{self.run_num}")
            print(f"{self.context}")
            print(f"Run ID: {self.run_id}")
            
            self.log(f"Step execution started")
            
        except Exception as e:
            raise StepError(f"Failed to start step run: {str(e)}")
    
    
    def log(self, message: str, level: str = "INFO"):
        """
        Log a message during step execution.
        
        Args:
            message: Log message
            level: Log level (INFO, WARNING, ERROR)
        """
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message
        }
        
        self.logs.append(log_entry)
        
        # Print to console
        emoji = {"INFO": "(i)", "WARNING": "(!)", "ERROR": "(x)"}.get(level, "•")
        print(f"{emoji} {message}")
    
    
    def checkpoint(self, data: Dict[str, Any]):
        """
        Save intermediate checkpoint data.
        
        Args:
            data: Dictionary of checkpoint data
        """
        
        if not self.run_id:
            raise StepError("Step not started - call start() first")
        
        checkpoint_entry = {
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        self.log(f"Checkpoint saved: {list(data.keys())}")
        
        # Could save to database or file if needed
        # For now, just track in memory
        if not hasattr(self, 'checkpoints'):
            self.checkpoints = []
        self.checkpoints.append(checkpoint_entry)
    
    
    def complete(self, output_data: Dict[str, Any] = None):
        """
        Mark step as completed successfully.
        
        Args:
            output_data: Final output data from step execution
        """
        
        if not self.run_id:
            raise StepError("Step not started - call start() first")
        
        try:
            # Prepare output
            final_output = output_data or {}
            
            # Add execution metadata
            final_output['_meta'] = {
                'run_num': self.run_num,
                'start_time': self.start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'logs': self.logs[-10:]  # Last 10 log entries
            }
            
            # Update database
            update_step_run(
                self.run_id,
                StepStatus.COMPLETED,
                output_data=final_output
            )
            
            duration = (datetime.now() - self.start_time).total_seconds()
            
            print("\n" + "="*60)
            print(f"STEP COMPLETED")
            print(f"   Run #{self.run_num} completed in {duration:.1f} seconds")
            print("="*60)
            
        except Exception as e:
            # If update fails, mark as failed
            self.fail(f"Failed to complete step: {str(e)}")
            raise
    
    
    def fail(self, error_message: str):
        """
        Mark step as failed.

        Args:
            error_message: Error message describing the failure
        """

        if not self.run_id:
            # Step never started, just log error
            print(f"Step failed: {error_message}")
            return

        try:
            # Update database
            update_step_run(
                self.run_id,
                StepStatus.FAILED,
                error_message=error_message
            )

            duration = (datetime.now() - self.start_time).total_seconds()

            print("\n" + "="*60)
            print(f"STEP FAILED")
            print(f"Run #{self.run_num} failed after {duration:.1f} seconds")
            print(f"Error: {error_message}")
            print("="*60)

            # Send Teams notification
            self._send_failure_notification(error_message)

        except Exception as e:
            print(f"Failed to update step status: {str(e)}")


    def _send_failure_notification(self, error_message: str):
        """
        Send Teams notification when step fails.

        Args:
            error_message: Error message from the failure
        """
        import os

        try:
            from helpers.teams_notification import TeamsNotificationClient

            teams = TeamsNotificationClient()

            # Build action buttons with notebook link and dashboard
            actions = []
            base_url = os.environ.get('TEAMS_DEFAULT_JUPYTERLAB_URL', '')
            if base_url:
                notebook_path = str(self.context.notebook_path)
                if 'workflows' in notebook_path:
                    rel_path = notebook_path.split('workflows')[-1].lstrip('/\\')
                    notebook_url = f"{base_url.rstrip('/')}/lab/tree/workflows/{rel_path}"
                    actions.append({"title": "Open Notebook", "url": notebook_url})

            dashboard_url = os.environ.get('TEAMS_DEFAULT_DASHBOARD_URL', '')
            if dashboard_url:
                actions.append({"title": "View Dashboard", "url": dashboard_url})

            # Truncate error for notification (keep first 500 chars)
            error_summary = error_message[:500] + "..." if len(error_message) > 500 else error_message

            teams.send_error(
                title=f"[{self.context.cycle_name}] Step Failed: {self.context.step_name}",
                message=f"**Cycle:** {self.context.cycle_name}\n"
                        f"**Stage:** {self.context.stage_name}\n"
                        f"**Step:** {self.context.step_name}\n\n"
                        f"**Error:**\n{error_summary}",
                actions=actions if actions else None
            )

        except Exception as e:
            # Don't let notification failure break the workflow
            print(f"Failed to send failure notification: {e}")
    
    
    def skip(self, reason: str = ""):
        """
        Mark step as skipped.
        
        Args:
            reason: Reason for skipping
        """
        
        if not self.run_id:
            raise StepError("Step not started - call start() first")
        
        try:
            output_data = {'skip_reason': reason} if reason else None

            update_step_run(
                self.run_id,
                StepStatus.SKIPPED,
                output_data=output_data
            )
            
            print(f"Step skipped: {reason}")
            
        except Exception as e:
            print(f"Failed to skip step: {str(e)}")
    
    
    def get_last_output(self) -> Optional[Dict[str, Any]]:
        """Get output data from the last completed run"""

        last_run = get_last_step_run(self.step_id)
        
        if not last_run or last_run['status'] != StepStatus.COMPLETED:
            return None
        
        return last_run.get('output_data')
    
    
    def __enter__(self):
        """Context manager entry - start step"""
        if not self.run_id:
            self.start()
        return self
    
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto complete or fail"""
        if exc_type is None:
            # No exception - complete successfully
            if self.run_id:
                self.complete()
        else:
            # Exception occurred - fail
            if self.run_id:
                self.fail(str(exc_val))
        
        # Return False to re-raise exception
        return False