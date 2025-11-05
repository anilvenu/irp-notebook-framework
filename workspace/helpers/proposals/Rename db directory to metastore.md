I would like to rename the workspace/helpers/db directory.
These are the places impacted based on codebase search

README.md:
  58:     │   ├── db/             # Database scripts

demo/prepare_data.py:
  44:     init_sql = project_root / 'workspace' / 'helpers' / 'db' / 'init_database.sql'
  45:     views_sql = project_root / 'workspace' / 'helpers' / 'db' / 'reporting_views.sql'

docs/DESIGN_DOCUMENT.md:
  604: - Database Schema: workspace/helpers/db/init_database.sql

workspace/helpers/database.py:
   294: The SQL file must be located in: workspace/helpers/db/[sql_file_name]
  1196:         sql_file = Path(__file__).parent / 'db' / sql_file_name

