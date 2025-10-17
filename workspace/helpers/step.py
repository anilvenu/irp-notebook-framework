"""
IRP Notebook Framework - Step Execution Tracker
"""

from typing import Optional, Dict, Any
from datetime import datetime
from .context import WorkContext
from . import database as db
from .constants import StepStatus, SYSTEM_USER


class StepError(Exception):
    """Custom exception for step execution errors"""
    pass


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
        self.step_info = db.get_step_info(self.step_id)
        
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
        last_run = db.get_last_step_run(self.step_id)
        
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
            self.run_id, self.run_num = db.create_step_run(
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
            db.update_step_run(
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
            db.update_step_run(
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
            
        except Exception as e:
            print(f"Failed to update step status: {str(e)}")
    
    
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
            
            db.update_step_run(
                self.run_id,
                StepStatus.SKIPPED,
                output_data=output_data
            )
            
            print(f"Step skipped: {reason}")
            
        except Exception as e:
            print(f"Failed to skip step: {str(e)}")
    
    
    def get_last_output(self) -> Optional[Dict[str, Any]]:
        """Get output data from the last completed run"""
        
        last_run = db.get_last_step_run(self.step_id)
        
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