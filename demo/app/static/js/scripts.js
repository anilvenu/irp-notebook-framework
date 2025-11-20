// Auto-refresh functionality
const AUTO_REFRESH_INTERVAL = 5 * 60 * 1000; // 5 minutes
let autoRefreshTimer = null;

function toggleAutoRefresh() {
    const checkbox = document.getElementById('auto-refresh-toggle');
    if (checkbox.checked) {
        startAutoRefresh();
        localStorage.setItem('autoRefresh', 'true');
    } else {
        stopAutoRefresh();
        localStorage.setItem('autoRefresh', 'false');
    }
}

function startAutoRefresh() {
    if (autoRefreshTimer) clearInterval(autoRefreshTimer);
    autoRefreshTimer = setInterval(() => location.reload(), AUTO_REFRESH_INTERVAL);
}

function stopAutoRefresh() {
    if (autoRefreshTimer) {
        clearInterval(autoRefreshTimer);
        autoRefreshTimer = null;
    }
}

// Initialize auto-refresh on page load
document.addEventListener('DOMContentLoaded', function() {
    const savedPref = localStorage.getItem('autoRefresh');
    const checkbox = document.getElementById('auto-refresh-toggle');
    if (checkbox && savedPref === 'true') {
        checkbox.checked = true;
        startAutoRefresh();
    }
});

function switchTab(tabName, event) {
    document.querySelectorAll('.tab-button').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    event.target.classList.add('active');
    document.getElementById(tabName + '-content').classList.add('active');
}

// Track sort state for each table: {tableId: {colIndex: 'asc'|'desc'}}
const tableSortState = {};

function sortTable(tableId, colIndex) {
    const table = document.getElementById(tableId);
    const tbody = table.querySelector('tbody');
    const thead = table.querySelector('thead');
    const rows = Array.from(tbody.querySelectorAll('tr'));

    if (rows.length === 0 || rows[0].cells.length === 1) return; // Skip if empty state

    // Initialize sort state for this table if not exists
    if (!tableSortState[tableId]) {
        tableSortState[tableId] = {};
    }

    // Determine sort direction (toggle between asc and desc)
    const currentSort = tableSortState[tableId][colIndex];
    const newSort = currentSort === 'asc' ? 'desc' : 'asc';
    tableSortState[tableId] = {}; // Reset other columns
    tableSortState[tableId][colIndex] = newSort;

    // Remove sort indicators from all headers
    thead.querySelectorAll('th').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
    });

    // Add sort indicator to current column
    const headerCells = thead.querySelectorAll('th');
    if (headerCells[colIndex]) {
        headerCells[colIndex].classList.add(`sort-${newSort}`);
    }

    // Sort rows
    rows.sort((a, b) => {
        let aVal = a.cells[colIndex].textContent.trim();
        let bVal = b.cells[colIndex].textContent.trim();

        // Extract numeric value if it's a number with parentheses or other formatting
        const aNum = parseFloat(aVal.replace(/[^0-9.-]/g, ''));
        const bNum = parseFloat(bVal.replace(/[^0-9.-]/g, ''));

        let comparison = 0;

        // Use numeric comparison if both are valid numbers
        if (!isNaN(aNum) && !isNaN(bNum)) {
            comparison = aNum - bNum;
        } else {
            // Otherwise use string comparison
            comparison = aVal.localeCompare(bVal, undefined, { numeric: true, sensitivity: 'base' });
        }

        return newSort === 'asc' ? comparison : -comparison;
    });

    rows.forEach(row => tbody.appendChild(row));
}

// Track active filters for each table: {tableId: {colIndex: Set/Array of values}}
const tableFilters = {};
// Track search box values for each table
const tableSearchValues = {};

function toggleFilterRow(tableId) {
    const table = document.getElementById(tableId);
    const filterRow = table.querySelector('.filter-row');

    if (!filterRow) {
        initializeFilters(tableId);
        return;
    }

    const isVisible = filterRow.style.display !== 'none';
    filterRow.style.display = isVisible ? 'none' : '';

    // Update button text
    const toggleBtn = table.closest('.table-container')?.querySelector('.filter-toggle-btn');
    if (toggleBtn) {
        toggleBtn.textContent = isVisible ? 'Show Filters' : 'Hide Filters';
    }
}

function initializeFilters(tableId) {
    const table = document.getElementById(tableId);
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    const headerRow = thead.querySelector('tr:first-child');

    if (!headerRow) return;

    // Create filter row if it doesn't exist
    let filterRow = thead.querySelector('.filter-row');
    if (!filterRow) {
        filterRow = document.createElement('tr');
        filterRow.className = 'filter-row';
        headerRow.parentNode.insertBefore(filterRow, headerRow.nextSibling);
    }

    const headers = headerRow.querySelectorAll('th');
    filterRow.innerHTML = ''; // Clear existing filters

    // Initialize filter state
    if (!tableFilters[tableId]) {
        tableFilters[tableId] = {};
    }

    headers.forEach((header, colIndex) => {
        const filterCell = document.createElement('th');
        const columnText = header.textContent.trim();

        // Skip columns that shouldn't have filters (like JSON columns)
        if (columnText.includes('JSON') || columnText === '') {
            filterCell.innerHTML = '<div class="filter-cell"></div>';
            filterRow.appendChild(filterCell);
            return;
        }

        // Get unique values for this column
        const uniqueValues = new Set();
        const rows = tbody.querySelectorAll('tr');

        rows.forEach(row => {
            if (row.cells[colIndex]) {
                let cellText = row.cells[colIndex].textContent.trim();

                // Extract text from status badges if present
                const badge = row.cells[colIndex].querySelector('.status-badge');
                if (badge) {
                    cellText = badge.textContent.trim();
                }

                if (cellText && cellText !== '-') {
                    uniqueValues.add(cellText);
                }
            }
        });

        // Create appropriate filter control
        const uniqueArray = Array.from(uniqueValues).sort();

        if (uniqueArray.length <= 15 && uniqueArray.length > 0) {
            // Use dropdown for categorical data
            const select = document.createElement('select');
            select.className = 'filter-select';
            select.multiple = true;
            select.size = 1;

            const defaultOption = document.createElement('option');
            defaultOption.value = '';
            defaultOption.textContent = 'All';
            defaultOption.selected = true;
            select.appendChild(defaultOption);

            uniqueArray.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
            });

            select.addEventListener('change', () => applyFilters(tableId));
            filterCell.innerHTML = '<div class="filter-cell"></div>';
            filterCell.querySelector('.filter-cell').appendChild(select);
        } else if (uniqueArray.length > 0) {
            // Use text input for other columns
            const input = document.createElement('input');
            input.type = 'text';
            input.className = 'filter-input';
            input.placeholder = 'Filter...';
            input.addEventListener('input', () => applyFilters(tableId));

            filterCell.innerHTML = '<div class="filter-cell"></div>';
            filterCell.querySelector('.filter-cell').appendChild(input);
        } else {
            filterCell.innerHTML = '<div class="filter-cell"></div>';
        }

        filterRow.appendChild(filterCell);
    });

    filterRow.style.display = '';
}

function applyFilters(tableId) {
    // Just call the unified filtering function
    applyAllFilters(tableId);
}

function clearFilters(tableId) {
    const table = document.getElementById(tableId);
    const filterRow = table.querySelector('.filter-row');

    // Clear column filters if they exist
    if (filterRow) {
        // Clear all filter inputs
        filterRow.querySelectorAll('.filter-input').forEach(input => {
            input.value = '';
        });

        // Reset all selects
        filterRow.querySelectorAll('.filter-select').forEach(select => {
            select.selectedIndex = 0;
            Array.from(select.options).forEach(option => {
                option.selected = option.value === '';
            });
        });

        // Update filter count
        updateFilterCount(tableId, 0);
    }

    // Clear search box value for this table
    tableSearchValues[tableId] = '';

    // Find and clear the actual search input
    // Pattern: jobsTable -> jobSearch, configsTable -> configSearch
    const searchId = tableId.replace(/sTable$/, 'Search');
    const searchInput = document.getElementById(searchId);
    if (searchInput) {
        searchInput.value = '';
    }

    // Re-apply filters (should show all rows now)
    applyAllFilters(tableId);
}

function updateFilterCount(tableId, count) {
    const container = document.getElementById(tableId)?.closest('.table-container');
    if (!container) return;

    let badge = container.querySelector('.filter-count-badge');

    if (count > 0) {
        if (!badge) {
            badge = document.createElement('span');
            badge.className = 'filter-count-badge';
            const toggleBtn = container.querySelector('.filter-toggle-btn');
            if (toggleBtn) {
                toggleBtn.appendChild(badge);
            }
        }
        badge.textContent = count;
        badge.style.display = 'inline';
    } else if (badge) {
        badge.style.display = 'none';
    }
}

function filterTable(searchId, tableId) {
    const input = document.getElementById(searchId);
    if (!input) {
        console.error('Search input not found:', searchId);
        return;
    }

    const searchValue = (input.value || '').toLowerCase();

    // Store search value for this table
    tableSearchValues[tableId] = searchValue;

    // Always use the unified filtering function
    applyAllFilters(tableId);
}

// Unified function that applies both search and column filters
function applyAllFilters(tableId) {
    const table = document.getElementById(tableId);
    if (!table) {
        console.error('Table not found:', tableId);
        return;
    }

    const tbody = table.querySelector('tbody');
    const rows = tbody.querySelectorAll('tr');
    const filterRow = table.querySelector('.filter-row');

    // Collect active column filters (if they exist)
    const activeFilters = [];
    if (filterRow) {
        const filterCells = filterRow.querySelectorAll('th');
        filterCells.forEach((filterCell, colIndex) => {
            const select = filterCell.querySelector('.filter-select');
            const input = filterCell.querySelector('.filter-input');

            if (select) {
                const selectedOptions = Array.from(select.selectedOptions)
                    .map(opt => opt.value)
                    .filter(val => val !== '');

                if (selectedOptions.length > 0) {
                    activeFilters.push({ colIndex, type: 'select', values: selectedOptions });
                }
            } else if (input && (input.value || '').trim()) {
                activeFilters.push({ colIndex, type: 'text', value: (input.value || '').trim().toLowerCase() });
            }
        });
    }

    // Get search box value
    const searchValue = tableSearchValues[tableId] || '';

    // Apply both filters to rows
    rows.forEach(row => {
        // Skip empty state rows
        if (row.cells.length === 1) {
            return;
        }

        let shouldShow = true;

        // Check column filters first
        for (const filter of activeFilters) {
            if (!row.cells[filter.colIndex]) {
                shouldShow = false;
                break;
            }

            let cellText = row.cells[filter.colIndex].textContent.trim();

            // Extract text from badge if present
            const badge = row.cells[filter.colIndex].querySelector('.status-badge');
            if (badge) {
                cellText = badge.textContent.trim();
            }

            if (filter.type === 'select') {
                if (!filter.values.includes(cellText)) {
                    shouldShow = false;
                    break;
                }
            } else if (filter.type === 'text') {
                if (!cellText.toLowerCase().includes(filter.value)) {
                    shouldShow = false;
                    break;
                }
            }
        }

        // Check search box filter (if row passed column filters)
        if (shouldShow && searchValue) {
            let rowText = row.textContent.toLowerCase();

            // Also search full JSON content from data-json attributes
            const jsonPreviews = row.querySelectorAll('.json-preview[data-json]');
            jsonPreviews.forEach(preview => {
                const jsonData = preview.getAttribute('data-json');
                if (jsonData) {
                    rowText += ' ' + jsonData.toLowerCase();
                }
            });

            shouldShow = rowText.includes(searchValue);
        }

        row.style.display = shouldShow ? '' : 'none';
    });

    // Update filter count badge
    if (filterRow) {
        updateFilterCount(tableId, activeFilters.length);
    }
}

// JSON Tooltip Management
let currentTooltip = null;

function showJsonTooltip(element, jsonData) {
    // Remove existing tooltip if any
    hideJsonTooltip();

    // Create tooltip
    const tooltip = document.createElement('div');
    tooltip.className = 'json-tooltip';
    tooltip.innerHTML = `
        <div class="json-tooltip-header">
            <span class="json-tooltip-title">Configuration JSON</span>
            <button class="json-tooltip-copy" onclick="copyJsonToClipboard(event)">Copy</button>
        </div>
        <div class="json-tooltip-content"></div>
    `;

    // Format and display JSON
    const content = tooltip.querySelector('.json-tooltip-content');
    try {
        const formatted = JSON.stringify(jsonData, null, 2);
        content.textContent = formatted;
    } catch (e) {
        content.textContent = JSON.stringify(jsonData);
    }

    document.body.appendChild(tooltip);

    // Position tooltip near the element
    const rect = element.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();

    let top = rect.bottom + 5;
    let left = rect.left;

    // Adjust if tooltip goes off-screen
    if (left + tooltipRect.width > window.innerWidth) {
        left = window.innerWidth - tooltipRect.width - 10;
    }
    if (top + tooltipRect.height > window.innerHeight) {
        top = rect.top - tooltipRect.height - 5;
    }

    tooltip.style.top = top + 'px';
    tooltip.style.left = left + 'px';
    tooltip.style.display = 'block';

    currentTooltip = tooltip;
}

function hideJsonTooltip() {
    if (currentTooltip) {
        currentTooltip.remove();
        currentTooltip = null;
    }
}

function copyJsonToClipboard(event) {
    event.stopPropagation();
    const tooltip = event.target.closest('.json-tooltip');
    const content = tooltip.querySelector('.json-tooltip-content').textContent;
    const button = event.target;

    navigator.clipboard.writeText(content).then(() => {
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.classList.add('copied');

        setTimeout(() => {
            button.textContent = originalText;
            button.classList.remove('copied');
        }, 2000);
    }).catch(err => {
        console.error('Failed to copy:', err);
        alert('Failed to copy to clipboard');
    });
}

// Close tooltip when clicking outside
document.addEventListener('click', (event) => {
    if (currentTooltip && !event.target.closest('.json-tooltip') && !event.target.closest('.json-preview')) {
        hideJsonTooltip();
    }
});

// Close tooltip on scroll
document.addEventListener('scroll', hideJsonTooltip, true);
