"""
Reusable HTML components for batch viewer
Modular design with shared CSS and parameterized components
"""

def get_shared_css():
    """Shared CSS for all HTML pages"""
    # Read .css file content and return as string
    with open("demo/static/style.css", "r") as f:
        return f.read()


def get_shared_javascript():
    """Shared JavaScript functions for all pages"""
    with open("demo/static/scripts.js", "r") as f:
        return f.read()


def generate_breadcrumb(cycle_name, batch_id=None, batch_type=None):
    """Generate breadcrumb navigation

    Args:
        cycle_name: Name of the cycle
        batch_id: Optional batch ID (if on batch page)
        batch_type: Optional batch type (if on batch page)

    Returns:
        HTML string for breadcrumb
    """
    if batch_id:
        # Batch page breadcrumb: Cycle > Batch {id}
        return f"""
        <div class="breadcrumb">
            <a href="../../index.html">{cycle_name}</a>
            <span class="breadcrumb-separator">›</span>
            <span class="breadcrumb-current">Batch {batch_id} - {batch_type}</span>
        </div>
        """
    else:
        # Cycle page breadcrumb: Just cycle name
        return f"""
        <div class="breadcrumb">
            <span class="breadcrumb-current">{cycle_name}</span>
        </div>
        """


def generate_header(title, subtitle=None):
    """Generate page header component

    Args:
        title: Main title (e.g., "Cycle: Analysis-2025-Q1")
        subtitle: Optional subtitle (e.g., batch name)

    Returns:
        HTML string for header
    """
    subtitle_html = f"<h2>{subtitle}</h2>" if subtitle else ""

    return f"""
    <div class="page-header">
        <h1>Moody's Risk Modeling Dashboard</h1>
        {subtitle_html}
    </div>
    """


def generate_card(title, value, sub_value=None):
    """Generate info card component

    Args:
        title: Card title
        value: Main value (can be HTML string with badges, etc.)
        sub_value: Optional sub-value text

    Returns:
        HTML string for card
    """
    sub_value_html = f'<div class="sub-value">{sub_value}</div>' if sub_value else ""

    return f"""
    <div class="info-card">
        <h3>{title}</h3>
        <div class="value">{value}</div>
        {sub_value_html}
    </div>
    """


def generate_table(table_id, columns, search_id=None):
    """Generate table component structure

    Args:
        table_id: Unique ID for the table
        columns: List of column definitions [{"label": "ID", "sortable": True}, ...]
        search_id: Optional ID for search box

    Returns:
        HTML string for table with search box
    """
    search_html = ""
    if search_id:
        search_html = f'<input type="text" class="search-box" id="{search_id}" placeholder="Search..." onkeyup="filterTable(\'{search_id}\', \'{table_id}\')">'

    headers = []
    for i, col in enumerate(columns):
        sortable = col.get('sortable', True)
        onclick = f'onclick="sortTable(\'{table_id}\', {i})"' if sortable else ''
        arrow = ' ↕' if sortable else ''
        headers.append(f'<th {onclick}>{col["label"]}{arrow}</th>')

    return f"""
    {search_html}
    <table id="{table_id}">
        <thead>
            <tr>
                {''.join(headers)}
            </tr>
        </thead>
        <tbody id="{table_id}Body">
        </tbody>
    </table>
    """


def generate_tabs(tabs):
    """Generate tabs component

    Args:
        tabs: List of tab definitions [{"id": "jobs", "label": "Jobs", "count": 10}, ...]

    Returns:
        HTML string for tabs
    """
    tab_buttons = []
    for i, tab in enumerate(tabs):
        active = "active" if i == 0 else ""
        count = f" ({tab['count']})" if 'count' in tab else ""
        tab_buttons.append(f'<button class="tab {active}" onclick="switchTab(\'{tab["id"]}\', event)">{ tab["label"]}{count}</button>')

    return f"""
    <div class="tabs">
        {''.join(tab_buttons)}
    </div>
    """

