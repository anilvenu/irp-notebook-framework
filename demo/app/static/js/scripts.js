    function switchTab(tabName, event) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

        event.target.classList.add('active');
        document.getElementById(tabName + '-content').classList.add('active');
    }

    function sortTable(tableId, colIndex) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));

        if (rows.length === 0 || rows[0].cells.length === 1) return; // Skip if empty state

        rows.sort((a, b) => {
            const aVal = a.cells[colIndex].textContent.trim();
            const bVal = b.cells[colIndex].textContent.trim();
            return aVal.localeCompare(bVal, undefined, { numeric: true });
        });

        rows.forEach(row => tbody.appendChild(row));
    }

    function filterTable(searchId, tableId) {
        const input = document.getElementById(searchId);
        const filter = input.value.toLowerCase();
        const table = document.getElementById(tableId);
        const rows = table.querySelectorAll('tbody tr');

        rows.forEach(row => {
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(filter) ? '' : 'none';
        });
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