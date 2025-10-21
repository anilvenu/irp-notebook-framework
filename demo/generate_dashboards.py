#!/usr/bin/env python3
"""
Dashboard Generation for Batch Viewer Demo

This script generates interactive HTML dashboards:
1. Queries batch data from database
2. Generates individual batch detail pages
3. Generates cycle dashboard pages

Requires: Data must be prepared first using prepare_data.py
"""

import sys
from pathlib import Path
import shutil

# Add workspace to path so we can import helpers
workspace_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(workspace_path))

from helpers import database as db

# Import modular HTML generators
from generate_batch_html import generate_batch_html
from generate_cycle_html import generate_cycle_dashboard_html


SCHEMA = 'demo'


def query_batch_data(batch_id):
    """Query all data for a specific batch"""
    # Get batch summary using v_irp_batch view for reporting_status
    summary_df = db.execute_query("""
        SELECT
            vb.batch_id,
            vb.batch_type,
            vb.batch_status,
            vb.reporting_status,
            c.cycle_name,
            st.stage_name,
            s.step_name,
            vb.created_ts,
            vb.submitted_ts,
            vb.completed_ts,
            vb.total_configs,
            vb.non_skipped_configs as active_configs,
            vb.fulfilled_configs,
            vb.unfulfilled_configs,
            vb.skipped_configs,
            vb.total_jobs,
            vb.finished_jobs,
            vb.skipped_jobs,
            (vb.total_jobs - COALESCE(vb.skipped_jobs, 0)) as active_jobs,
            (vb.total_jobs - COALESCE(vb.finished_jobs, 0) - COALESCE(vb.skipped_jobs, 0)) as unfinished_jobs
        FROM v_irp_batch vb
        JOIN irp_configuration cfg ON vb.configuration_id = cfg.id
        JOIN irp_cycle c ON cfg.cycle_id = c.id
        JOIN irp_step s ON vb.step_id = s.id
        JOIN irp_stage st ON s.stage_id = st.id
        WHERE vb.batch_id = %s
    """, (batch_id,), schema=SCHEMA)

    if summary_df.empty:
        print(f"✗ Batch {batch_id} not found")
        return None

    summary = summary_df.to_dict('records')[0]

    # Get jobs
    jobs_df = db.execute_query("""
        SELECT
            j.id,
            j.moodys_workflow_id,
            j.status,
            j.skipped,
            j.report_status,
            j.next_best_action,
            j.age_hours,
            j.is_successful,
            j.needs_attention,
            j.parent_job_id,
            j.job_configuration_id,
            j.overridden,
            j.override_reason_txt,
            j.submitted_ts,
            j.completed_ts,
            j.created_ts,
            jc.job_configuration_data,
            parent_j.status as parent_job_status,
            parent_j.moodys_workflow_id as parent_moodys_id
        FROM v_irp_job j
        LEFT JOIN irp_job_configuration jc ON j.job_configuration_id = jc.id
        LEFT JOIN irp_job parent_j ON j.parent_job_id = parent_j.id
        WHERE j.batch_id = %s
        ORDER BY j.id
    """, (batch_id,), schema=SCHEMA)

    jobs = jobs_df.to_dict('records')

    # Get configurations with active job details
    configs_df = db.execute_query("""
        SELECT
            jc.id as config_id,
            jc.job_configuration_data,
            jc.skipped,
            jc.overridden,
            vjc.config_report_status,
            vjc.total_jobs,
            vjc.unsubmitted_jobs,
            vjc.finished_jobs,
            vjc.failed_jobs,
            vjc.cancelled_jobs,
            vjc.error_jobs,
            vjc.progress_percent,
            vjc.has_failures,
            vjc.has_errors,
            vjc.has_unsubmitted,
            -- Count active jobs (not terminal)
            (vjc.total_jobs - vjc.finished_jobs - vjc.failed_jobs - vjc.cancelled_jobs) as active_jobs
        FROM v_irp_job_configuration vjc
        JOIN irp_job_configuration jc ON vjc.job_configuration_id = jc.id
        WHERE vjc.batch_id = %s
        ORDER BY jc.id
    """, (batch_id,), schema=SCHEMA)

    configs = configs_df.to_dict('records')

    return {
        'summary': summary,
        'jobs': jobs,
        'configs': configs
    }


def main():
    """Main execution"""
    print("="*60)
    print("DASHBOARD GENERATION FOR BATCH VIEWER DEMO")
    print("="*60)

    # Get list of all batches
    batches_df = db.execute_query("SELECT id, batch_type FROM irp_batch ORDER BY id", schema=SCHEMA)

    if batches_df.empty:
        print("✗ No batches found. Did you run prepare_data.py first?")
        return 1

    batches = batches_df.to_dict('records')
    output_dir = Path(__file__).parent / 'files' / 'html_output'

    # Clean existing cycle directories for sanity (before generating anything)
    cycle_dir = output_dir / 'cycle'
    if cycle_dir.exists():
        shutil.rmtree(cycle_dir)
        print("✓ Cleaned existing cycle output folder")

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nFound {len(batches)} batches to process")

    # Generate batch HTML files
    print("\n" + "="*60)
    print("GENERATING BATCH DETAIL PAGES")
    print("="*60)

    generated_files = []
    cycles_data = {}  # Track cycle data for dashboard generation

    for batch in batches:
        batch_id = batch['id']
        batch_type = batch['batch_type']

        print(f"\nProcessing batch {batch_id}: {batch_type}...")

        # Query data
        data = query_batch_data(batch_id)
        if not data:
            print(f"  ✗ Failed to query data for batch {batch_id}")
            continue

        # Generate HTML using modular components
        html_content = generate_batch_html(batch_id, data)

        # Create directory structure: cycle/{cycle_name}/batch/{batch_id}/index.html
        cycle_name = data['summary']['cycle_name']
        batch_dir = output_dir / 'cycle' / cycle_name / 'batch' / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)

        output_file = batch_dir / 'index.html'

        # Write to file
        with open(output_file, 'w') as f:
            f.write(html_content)

        generated_files.append(str(output_file))
        print(f"  ✓ Generated: cycle/{cycle_name}/batch/{batch_id}/index.html")

        # Query enhanced stats from v_irp_batch for cycle dashboard
        batch_view_df = db.execute_query("""
            SELECT
                fulfilled_configs,
                unfulfilled_configs,
                skipped_configs,
                finished_jobs,
                failed_jobs,
                error_jobs,
                skipped_jobs
            FROM v_irp_batch
            WHERE batch_id = %s
        """, (batch_id,), schema=SCHEMA)

        enhanced_stats = batch_view_df.to_dict('records')[0] if not batch_view_df.empty else {}

        # Track cycle data for dashboard
        if cycle_name not in cycles_data:
            cycles_data[cycle_name] = []
        cycles_data[cycle_name].append({
            'batch_id': batch_id,
            'batch_type': batch_type,
            'batch_status': data['summary']['reporting_status'], 
            'total_jobs': data['summary']['total_jobs'],
            'total_configs': data['summary']['total_configs'],
            'active_configs': data['summary']['active_configs'],
            'fulfilled_configs': enhanced_stats.get('fulfilled_configs', 0),
            'unfulfilled_configs': enhanced_stats.get('unfulfilled_configs', 0),
            'skipped_configs': enhanced_stats.get('skipped_configs', 0),
            'finished_jobs': enhanced_stats.get('finished_jobs', 0),
            'failed_jobs': enhanced_stats.get('failed_jobs', 0),
            'error_jobs': enhanced_stats.get('error_jobs', 0),
            'skipped_jobs': enhanced_stats.get('skipped_jobs', 0),
            'created_ts': data['summary']['created_ts']
        })

    # Generate cycle dashboards
    print("\n" + "="*60)
    print("GENERATING CYCLE DASHBOARDS")
    print("="*60)

    for cycle_name, cycle_batches in cycles_data.items():
        # Generate enhanced dashboard (batch folders already exist with data)
        dashboard_html = generate_cycle_dashboard_html(cycle_name, cycle_batches)
        dashboard_dir = output_dir / 'cycle' / cycle_name
        dashboard_file = dashboard_dir / 'index.html'

        # Write dashboard file (batch/ folder already exists with data)
        with open(dashboard_file, 'w') as f:
            f.write(dashboard_html)

        print(f"✓ Generated dashboard: cycle/{cycle_name}/index.html")

    # Summary
    print("\n" + "="*60)
    print("DASHBOARD GENERATION COMPLETE")
    print("="*60)
    print(f"✓ Generated {len(generated_files)} batch HTML files")
    print(f"✓ Generated {len(cycles_data)} cycle dashboards")
    print(f"\nDirectory structure:")
    for cycle_name in cycles_data.keys():
        print(f"  cycle/{cycle_name}/")
        print(f"    ├── index.html (Cycle Dashboard)")
        print(f"    └── batch/")
        for batch in cycles_data[cycle_name]:
            print(f"        └── {batch['batch_id']}/index.html")
    print(f"\nOpen cycle/{list(cycles_data.keys())[0]}/index.html to start")

    return 0


if __name__ == '__main__':
    sys.exit(main())
