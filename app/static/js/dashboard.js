$(document).ready(function () {
    // --- Track selected event IDs ---
    let selectedIds = new Set();

    // Initialize DataTables for the Events Table
    const eventsTable = $('#eventsTable').DataTable({
        ajax: {
            url: '/api/events', // Fetch data from the Flask API
            dataSrc: 'data'     // Assumes response is { "data": [...] }
        },
        columns: [
            {
                data: null,
                orderable: false,
                className: 'select-checkbox text-center',
                defaultContent: '',
                render: function (data, type, row, meta) {
                    // Use a unique identifier, e.g., event ID or a combo of fields
                    return `<input type="checkbox" class="event-select" value="${row.id || row.starttimestamp || meta.row}">`;
                }
            },
            { data: 'starttimestamp', title: 'Start' },
            { data: 'job_name', title: 'Backup Title' },
            { data: 'backup_type', title: 'Type' },
            {
                // This column will render the link to the manifest or just the event text
                data: 'event',
                title: 'Event/Manifest',
                render: function (data, type, row) { // 'data' is now the 'event' text
                    // Check if backup_set_id AND job_name are present in the row object
                    if (row.backup_set_id && row.job_name) {
                        // Construct the correct manifest URL using row.backup_set_id
                        const manifestUrl = `/manifest/${encodeURIComponent(row.job_name)}/${encodeURIComponent(row.backup_set_id)}`;
                        // Use the event text (passed as 'data') or job name as link text
                        const linkText = data || row.job_name || 'View Manifest';
                        return `<a href="${manifestUrl}">${linkText}</a>`;
                    } else {
                        // If no backup_set_id/job_name, just display the event text (passed as 'data') 
                        return data || ''; // Use 'data' which is row.event
                    }
                },
                orderable: false
            },
            {
                data: null, // <--- This is required!
                title: 'Options',
                className: 'text-center',
                orderable: false,
                render: function (data, type, row) {
                    // Encrypt icon
                    let encryptIcon = (row.encrypt === true || row.encrypt === "true" || row.encrypt === 1)
                        ? '<i class="fa fa-lock text-warning me-2" title="Encryption enabled"></i>'
                        : '<i class="fa fa-lock-open text-secondary me-2" title="Encryption disabled"></i>';
                    // Sync icon
                    let syncIcon = (row.sync === true || row.sync === "true" || row.sync === 1)
                        ? '<i class="fa fa-cloud-upload-alt text-success" title="Sync enabled"></i>'
                        : '<i class="fa fa-cloud-upload-alt text-secondary" title="Sync disabled"></i>';
                    return encryptIcon + syncIcon;
                }
            },
            { data: 'runtime', title: 'Run Time' },
            { data: 'status', title: 'Status' }
        ],
        columnDefs: [
            { targets: [2, 3, 5, 6, 7], className: 'text-center' },
            { targets: 1, responsivePriority: 3 },
            { targets: 3, responsivePriority: 2 },
            { targets: [2, 4], responsivePriority: 1 },
            { targets: 7, responsivePriority: 4 },
            { targets: [0, 5, 6], responsivePriority: 100 },
            {
                targets: 7,
                createdCell: function (td, cellData, rowData, row, col) {
                    if (cellData && cellData.toLowerCase() === 'error') {
                        $(td).css('background-color', 'rgba(129, 56, 62, 0.65)');
                    }
                }
            }
        ],
        language: {
            search: "Filter events:",
            lengthMenu: "Show _MENU_ events",
            info: "Showing _START_ to _END_ of _TOTAL_ events",
        },
        responsive: true,
        paging: true,
        searching: true,
        ordering: true,
        order: [[1, 'desc']] // Order by Start Timestamp descending by default
    });

    // Purge dropdown logic
    $(document).on('click', '.purge-action', function (e) {
        e.preventDefault();
        const status = $(this).data('status');
        if (confirm(`Are you sure you want to purge all "${status}" events?`)) {
            fetch(`/purge_events/${status}`, {method: 'POST'})
                .then(resp => resp.json())
                .then(data => {
                    alert(data.message);
                    // Reload the events table only, not the whole page
                    if ($('#eventsTable').length && $.fn.DataTable.isDataTable('#eventsTable')) {
                        $('#eventsTable').DataTable().ajax.reload(null, false);
                    } else {
                        location.reload();
                    }
                });
        }
    });

    // --- When a checkbox is clicked, update the Set ---
    $('#eventsTable').on('change', 'input.event-select', function () {
        const id = $(this).val();
        if (this.checked) {
            selectedIds.add(id);
        } else {
            selectedIds.delete(id);
        }
    });

    // --- When the table is redrawn, restore checked state ---
    $('#eventsTable').on('draw.dt', function () {
        $('#eventsTable tbody input.event-select').each(function () {
            if (selectedIds.has($(this).val())) {
                this.checked = true;
            }
        });
        // Optionally, update the "select all" checkbox
        $('#select-all-events').prop('checked',
            $('#eventsTable tbody input.event-select').length > 0 &&
            $('#eventsTable tbody input.event-select:checked').length === $('#eventsTable tbody input.event-select').length
        );
    });

    // --- Select/Deselect all checkboxes ---
    $('#eventsTable').on('change', '#select-all-events', function () {
        const checked = this.checked;
        $('#eventsTable tbody input.event-select').each(function () {
            this.checked = checked;
            const id = $(this).val();
            if (checked) {
                selectedIds.add(id);
            } else {
                selectedIds.delete(id);
            }
        });
    });

    // --- Delete selected events ---
    $('#delete-selected-btn').on('click', function () {
        if (selectedIds.size === 0) {
            alert('No events selected.');
            return;
        }
        if (!confirm(`Delete ${selectedIds.size} selected event(s)?`)) return;

        fetch('/api/events/delete', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ids: Array.from(selectedIds)})
        })
        .then(resp => resp.json())
        .then(data => {
            alert(data.message);
            selectedIds.clear();
            eventsTable.ajax.reload(null, false);
        });
    });

    // --- Persistent Drag-and-Drop for Dashboard Cards ---
    const dashboardRow = document.getElementById('dashboard-cards-row');
    const CARD_ORDER_KEY = "dashboardCardOrder";

    // Restore card order from localStorage
    function restoreCardOrder() {
        const order = JSON.parse(localStorage.getItem(CARD_ORDER_KEY) || "[]");
        if (order.length && dashboardRow) {
            // Get current cards as an array
            const cards = Array.from(dashboardRow.children);
            // Sort cards according to saved order
            order.forEach(cardId => {
                const card = cards.find(c => c.id === cardId);
                if (card) dashboardRow.appendChild(card);
            });
        }
    }

    // Save card order to localStorage
    function saveCardOrder() {
        if (!dashboardRow) return;
        const order = Array.from(dashboardRow.children).map(card => card.id);
        localStorage.setItem(CARD_ORDER_KEY, JSON.stringify(order));
    }

    // Assign unique IDs to each dashboard card if not already set
    if (dashboardRow) {
        Array.from(dashboardRow.children).forEach((card, idx) => {
            if (!card.id) card.id = `dashboard-card-${idx + 1}`;
        });
        restoreCardOrder();
    }

    // Make the dashboard cards row sortable
    if (window.Sortable && dashboardRow) {
        new Sortable(dashboardRow, {
            animation: 150,
            handle: '.card-header',
            draggable: '.dashboard-card',
            ghostClass: 'sortable-ghost',
            onEnd: saveCardOrder // Save order after drag-and-drop
        });
    } else {
        console.warn("SortableJS is not loaded. Drag-and-drop for dashboard cards will not work.");
    }

    // --- Disk Usage Chart ---
    let diskUsageChart = null;
    function initializeDiskUsageChart() {
        fetch('/api/disk_usage')
            .then(response => response.json())
            .then(data => {
                console.log("Disk Usage Data:", data);
                if (!Array.isArray(data) || data.length === 0) {
                    console.warn("No disk usage data received or data is empty.");
                    // Optionally display a message on the page
                    const canvas = document.getElementById('diskUsageChart');
                    if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#888';
                        ctx.textAlign = 'center';
                        ctx.fillText('No disk usage data available.', canvas.width / 2, canvas.height / 2);
                    }
                    return;
                }

                const labels = data.map(d => d.label || d.drive || 'Unknown Drive');
                const usedData = data.map(d => d.used_gib || 0);
                const freeData = data.map(d => d.free_gib || 0);

                const ctx = document.getElementById('diskUsageChart')?.getContext('2d');
                if (!ctx) {
                    console.error("Disk usage chart canvas not found.");
                    return;
                }

                // Destroy previous chart instance if it exists
                if (diskUsageChart) {
                    diskUsageChart.destroy();
                }

                diskUsageChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Used GiB',
                                data: usedData,
                                backgroundColor: 'rgba(255, 99, 132, 0.6)',
                                borderColor: 'rgba(255, 99, 132, 1)',
                                borderWidth: 1
                            },
                            {
                                label: 'Free GiB',
                                data: freeData,
                                backgroundColor: 'rgba(75, 192, 192, 0.6)',
                                borderColor: 'rgba(75, 192, 192, 1)',
                                borderWidth: 1
                            }
                        ]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { position: 'top' },
                            tooltip: {
                                callbacks: {
                                    label: function (context) {
                                        const total = (usedData[context.dataIndex] || 0) + (freeData[context.dataIndex] || 0);
                                        const percent = total > 0 ? ((context.raw / total) * 100).toFixed(1) : 0;
                                        return `${context.dataset.label}: ${context.raw.toFixed(1)} GiB (${percent}%)`;
                                    }
                                }
                            },
                            datalabels: { // Requires chartjs-plugin-datalabels
                                anchor: 'center',
                                align: 'center',
                                formatter: function (value, context) {
                                    const total = (usedData[context.dataIndex] || 0) + (freeData[context.dataIndex] || 0);
                                    if (context.dataset.label === 'Used GiB' && total > 0) {
                                        const percent = ((value / total) * 100).toFixed(1);
                                        return `${percent}%`;
                                    }
                                    return null;
                                },
                                color: '#f4f4f4'
                            }
                        },
                        scales: {
                            x: {
                                stacked: true,
                                grid: { color: 'rgba(255,255,255,0.15)' },
                                title: { display: true, text: 'GiB' }
                            },
                            y: {
                                stacked: true,
                                grid: { color: 'rgba(255,255,255,0.15)' }
                            }
                        }
                    },
                    plugins: [ChartDataLabels] // Ensure plugin is registered globally or passed here
                });
            })
            .catch(error => {
                console.error("Failed to load disk usage data:", error);
                const canvas = document.getElementById('diskUsageChart');
                 if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#dc3545'; // Error color
                        ctx.textAlign = 'center';
                        ctx.fillText('Error loading disk usage data.', canvas.width / 2, canvas.height / 2);
                    }
            });
    }

    // --- S3 Usage Chart ---
    let s3UsageChart = null;
    function initializeS3UsageChart() {
        fetch('/api/s3_usage')
            .then(response => response.json())
            .then(data => {
                console.log("S3 Usage Data:", data);
                 if (!Array.isArray(data) || data.length === 0) {
                    console.warn("No S3 usage data received or data is empty.");
                    const canvas = document.getElementById('s3UsageChart');
                    if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#888';
                        ctx.textAlign = 'center';
                        ctx.fillText('No S3 usage data available.', canvas.width / 2, canvas.height / 2);
                    }
                    return;
                }

                const labels = data.map(bucket => bucket.label || bucket.bucket || 'Unknown Bucket');
                const datasets = [];
                const totalUsage = Array(labels.length).fill(0); // Initialize totals array

                // Function to generate a distinct color (simple version)
                const colorCache = {};
                let colorIndex = 0;
                const baseColors = [
                    [114, 147, 203], [225, 151, 76], [132, 186, 91], [211, 94, 96],
                    [128, 133, 133], [144, 103, 167], [171, 104, 87], [204, 194, 16]
                ];
                function getColor(label) {
                    if (!colorCache[label]) {
                        const color = baseColors[colorIndex % baseColors.length];
                        colorCache[label] = `rgba(${color[0]}, ${color[1]}, ${color[2]}, 0.6)`;
                        colorIndex++;
                    }
                    return colorCache[label];
                }

                data.forEach((bucket, bucketIndex) => {
                    if (bucket.error) {
                        console.error(`Error fetching S3 data for bucket ${bucket.bucket}: ${bucket.error}`);
                        // Optionally skip or represent error state in the chart
                        return; // Skip this bucket if there was an error
                    }
                    if (!bucket.prefixes) return; // Skip if no prefixes

                    bucket.prefixes.forEach(prefix => {
                        const prefixSize = prefix.size_gib || 0;
                        totalUsage[bucketIndex] += prefixSize;
                        const prefixColor = getColor(prefix.prefix);

                        datasets.push({
                            label: prefix.prefix || 'Root',
                            data: labels.map((_, index) => (index === bucketIndex ? prefixSize : 0)),
                            backgroundColor: prefixColor,
                            borderColor: prefixColor.replace('0.6', '1'), // Make border opaque
                            borderWidth: 1
                        });

                        if (!prefix.sub_prefixes) return; // Skip if no sub-prefixes

                        prefix.sub_prefixes.forEach(sub_prefix => {
                            const subPrefixSize = sub_prefix.size_gib || 0;
                            totalUsage[bucketIndex] += subPrefixSize;
                            const subPrefixColor = getColor(sub_prefix.prefix); // Could use a different color scheme

                            datasets.push({
                                label: sub_prefix.prefix || 'Unknown Sub-Prefix',
                                data: labels.map((_, index) => (index === bucketIndex ? subPrefixSize : 0)),
                                backgroundColor: subPrefixColor,
                                borderColor: subPrefixColor.replace('0.6', '1'),
                                borderWidth: 1
                            });
                        });
                    });
                });

                const ctx = document.getElementById('s3UsageChart')?.getContext('2d');
                 if (!ctx) {
                    console.error("S3 usage chart canvas not found.");
                    return;
                }

                // Destroy previous chart instance if it exists
                if (s3UsageChart) {
                    s3UsageChart.destroy();
                }

                s3UsageChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: datasets
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false }, // Keep legend hidden for potentially many prefixes
                            tooltip: {
                                callbacks: {
                                    label: function (context) {
                                        return `${context.dataset.label}: ${context.raw.toFixed(2)} GiB`;
                                    }
                                }
                            },
                            datalabels: { // Requires chartjs-plugin-datalabels
                                anchor: 'end', // Show total at the end of the stack
                                align: 'end',
                                formatter: function (value, context) {
                                    // Check if this is the last dataset for the current bar stack
                                    const isLastDataset = context.chart.data.datasets
                                        .filter(ds => ds.data[context.dataIndex] > 0) // Consider only datasets with value for this index
                                        .slice(-1)[0] === context.dataset; // Is it the last one?

                                    if (isLastDataset) {
                                        return `${totalUsage[context.dataIndex].toFixed(2)} GiB`;
                                    }
                                    return null; // No label for intermediate segments
                                },
                                color: '#fff',
                                // Optional: Add background for better readability
                                // backgroundColor: 'rgba(0, 0, 0, 0.5)',
                                // borderRadius: 4,
                                // padding: 4
                            }
                        },
                        scales: {
                            x: {
                                stacked: true,
                                grid: { color: 'rgba(255,255,255,0.15)' },
                                title: { display: true, text: 'GiB' }
                            },
                            y: {
                                stacked: true,
                                grid: { color: 'rgba(255,255,255,0.15)' }
                            }
                        }
                    },
                    plugins: [ChartDataLabels] // Ensure plugin is registered globally or passed here
                });
            })
            .catch(error => {
                console.error("Failed to load S3 usage data:", error);
                 const canvas = document.getElementById('s3UsageChart');
                 if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#dc3545'; // Error color
                        ctx.textAlign = 'center';
                        ctx.fillText('Error loading S3 usage data.', canvas.width / 2, canvas.height / 2);
                    }
            });
    }

    // Initialize charts
    initializeDiskUsageChart();
    initializeS3UsageChart();

    // Optional: Refresh charts periodically (if needed)
    // setInterval(initializeDiskUsageChart, 60000); // Refresh disk usage every minute
    // setInterval(initializeS3UsageChart, 300000); // Refresh S3 usage every 5 minutes

    // Refresh the table data periodically
    setInterval(function () {
        eventsTable.ajax.reload(null, false);
    }, 10000); // every 10 seconds (adjust as needed)

}); // End document ready