# Batch Management Tools

Tools for managing and monitoring IRP batch job execution.

## Monitor Active Jobs.ipynb

Automated job status monitoring notebook that polls Moody's Risk Modeler API and updates database job statuses.

### What It Does

1. **Finds Active Batches**: Queries for all batches with status = 'ACTIVE'
2. **Polls Moody's API**: Calls Moody's Risk Data API for current job statuses
3. **Updates Database**: Updates `irp_job` table with latest statuses
4. **Reconciles Batches**: Determines overall batch status based on job states
5. **Logs History**: Records all status changes in tracking logs
6. **Displays Summary**: Shows monitoring results and next run recommendation

### Manual Execution

1. Open `Monitor Active Jobs.ipynb` in JupyterLab
2. Click "Run" → "Run All Cells"
3. Review the summary output
4. Check for any errors or warnings

### Scheduled Execution

For automated monitoring, schedule the notebook to run periodically.

#### Option 1: Cron (Linux/Mac)

```bash
# Run every 10 minutes during business hours (8am-6pm, Monday-Friday)
*/10 8-18 * * 1-5 cd /path/to/workspace && jupyter nbconvert --to notebook --execute "workflows/_Tools/Batch Management/Monitor Active Jobs.ipynb"
```

**To set up:**

```bash
# Edit crontab
crontab -e

# Add the line above
# Save and exit
```

**View logs:**
```bash
# Cron logs typically go to syslog
tail -f /var/log/syslog | grep jupyter
```

#### Option 2: Task Scheduler (Windows)

**Setup Steps:**

1. Open **Task Scheduler** (search in Start menu)
2. Click **Create Basic Task**
3. Name: "Monitor IRP Jobs"
4. Description: "Polls Moody's API for job status updates every 10 minutes"
5. Trigger: **Daily**
   - Recur every: 1 days
   - Click **Advanced Settings**
   - Check: **Repeat task every: 10 minutes**
   - For a duration of: **Indefinitely**
6. Action: **Start a program**
   - Program/script: `jupyter`
   - Add arguments: `nbconvert --to notebook --execute "workflows/_Tools/Batch Management/Monitor Active Jobs.ipynb"`
   - Start in: `C:\path\to\workspace`
7. Click **Finish**

**View execution history:**
- Task Scheduler → Task Scheduler Library → Monitor IRP Jobs → History tab

#### Option 3: Python Background Service

Create `monitor_jobs_service.py`:

```python
#!/usr/bin/env python3
"""
Continuous monitoring service for IRP jobs.
Runs the monitoring notebook every 10 minutes.
"""
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('monitor_jobs.log'),
        logging.StreamHandler()
    ]
)

# Configuration
WORKSPACE_DIR = Path(__file__).parent.parent.parent
NOTEBOOK_PATH = WORKSPACE_DIR / "workflows/_Tools/Batch Management/Monitor Active Jobs.ipynb"
INTERVAL_SECONDS = 600  # 10 minutes

def run_monitoring():
    """Execute the monitoring notebook."""
    try:
        logging.info("Starting job monitoring...")

        result = subprocess.run([
            'jupyter', 'nbconvert',
            '--to', 'notebook',
            '--execute',
            str(NOTEBOOK_PATH)
        ], cwd=WORKSPACE_DIR, capture_output=True, text=True)

        if result.returncode == 0:
            logging.info("Monitoring completed successfully")
        else:
            logging.error(f"Monitoring failed: {result.stderr}")

    except Exception as e:
        logging.error(f"Error running monitoring: {e}")

def main():
    """Main service loop."""
    logging.info("IRP Job Monitoring Service started")
    logging.info(f"Monitoring interval: {INTERVAL_SECONDS} seconds")

    while True:
        try:
            run_monitoring()
            time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logging.info("Service stopped by user")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    main()
```

**Run as service:**

```bash
# Linux (systemd)
sudo cp monitor_jobs_service.py /usr/local/bin/
sudo chmod +x /usr/local/bin/monitor_jobs_service.py

# Create systemd service file
sudo nano /etc/systemd/system/irp-monitor.service

# Add:
# [Unit]
# Description=IRP Job Monitoring Service
# After=network.target
#
# [Service]
# Type=simple
# User=your_user
# WorkingDirectory=/path/to/workspace
# ExecStart=/usr/bin/python3 /usr/local/bin/monitor_jobs_service.py
# Restart=always
#
# [Install]
# WantedBy=multi-user.target

# Enable and start
sudo systemctl enable irp-monitor
sudo systemctl start irp-monitor

# Check status
sudo systemctl status irp-monitor

# View logs
sudo journalctl -u irp-monitor -f
```

### Monitoring Frequency Recommendations

- **Development/Testing**: Every 5 minutes
- **Production**: Every 10 minutes
- **Long-running batches**: Every 15-30 minutes
- **After business hours**: Can be less frequent or disabled

### Output Files

When run via `jupyter nbconvert --execute`, the notebook creates:
- Executed notebook with outputs (timestamped)
- Located in same directory as original notebook

### Troubleshooting

#### No active batches found
- Normal if all batches are completed
- Check if any batches are in INITIATED status (not yet submitted)

#### Polling errors
- **API authentication**: Check Moody's API credentials in environment variables
- **Network connectivity**: Verify connection to Moody's API endpoint
- **Invalid workflow ID**: Job may not have been submitted correctly

#### Reconciliation errors
- Check database connectivity
- Verify job and batch IDs exist in database

#### Jobs stuck in SUBMITTED/QUEUED/PENDING
- May be waiting in Moody's queue
- Check Moody's platform for queue status
- Consider longer monitoring intervals

### Environment Variables Required

The monitoring notebook requires these environment variables:

```bash
RISK_MODELER_BASE_URL=https://api-euw1.rms-ppe.com
RISK_MODELER_API_KEY=your_api_key_here
RISK_MODELER_RESOURCE_GROUP_ID=your_resource_group_id
```

These should be configured in:
- Docker environment (`docker-compose.yml`)
- System environment (for scheduled execution)
- `.env` file (loaded by application)

### Monitoring Dashboard

The notebook output provides:

**Summary Metrics:**
- Batches found
- Jobs tracked
- Status changes
- Errors encountered
- Execution duration

**Status Transitions:**
- Table showing job_id, batch_id, old_status → new_status

**Batch Reconciliation:**
- Completed batches
- Failed batches
- Still active batches

**Error Details:**
- Polling errors (API failures)
- Reconciliation errors (database issues)

### Integration with Workflows

The monitoring notebook is standalone and can run independently of workflow execution. However, workflow notebooks can also include inline polling:

See: `Stage_03_Data_Import/Step_01_Import_Base_Data.ipynb` for optional inline polling after batch submission.

### Best Practices

1. **Don't run multiple instances simultaneously** - can cause database conflicts
2. **Monitor the monitor** - set up alerts if monitoring fails repeatedly
3. **Archive old logs** - monitoring creates tracking logs that should be cleaned up periodically
4. **Test before scheduling** - run manually first to verify configuration
5. **Adjust frequency as needed** - balance between responsiveness and API load

### Support

For issues or questions:
- Check JupyterLab execution logs
- Review database tracking logs (`irp_job_tracking_log`)
- Verify Moody's API status
- Contact system administrator
