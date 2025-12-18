// Index page JavaScript - Combined dashboard and monitor badge functionality

$(document).ready(function () {
    // --- Monitor Badges - Load discovered instance badges immediately and then periodically ---
    updateMonitorBadges();
    setInterval(updateMonitorBadges, 30000); // Update every 30 seconds
    
    // --- Dashboard Events Table ---
    // Track selected event IDs
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
                title: 'Event',
                render: function (data, type, row) { // 'data' is now the 'event' text
                    // Check if this is a backup type that has a manifest
                    const hasManifest = ['full', 'incremental', 'differential', 'diff', 'dryrun'].includes(row.backup_type?.toLowerCase());
                    
                    // Check for failed status - failed jobs shouldn't have manifest links
                    // Add "skipped" to the list of statuses that don't get links
                    const skipLink = row.status?.toLowerCase() === 'error' || 
                                     row.status?.toLowerCase() === 'failed' ||
                                     row.status?.toLowerCase() === 'running' ||
                                     row.status?.toLowerCase() === 'skipped';
                    
                    // Only create links for backup types that have manifests AND didn't fail or get skipped
                    if (row.job_name && row.set_name && hasManifest && !skipLink) {
                        // Construct the correct manifest URL using row.set_name
                        const manifestUrl = `/manifest/${encodeURIComponent(row.job_name)}/${encodeURIComponent(row.set_name)}`;
                        // Use the event text (passed as 'data') or job name as link text
                        const linkText = data || row.job_name || 'View Manifest';
                        return `<a href="${manifestUrl}">${linkText}</a>`;
                    } else {
                        // For other event types (restore, error, skipped) or failed jobs, just display the event text
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
            handle: '.fa-arrows-alt',
            draggable: '.dashboard-card',
            ghostClass: 'sortable-ghost',
            onEnd: saveCardOrder // Save order after drag-and-drop
        });
    } else {
        console.warn("SortableJS is not loaded. Drag-and-drop for dashboard cards will not work.");
    }

    // --- Charts Initialization ---
    let diskUsageChart = null;
    function initializeDiskUsageChart() {
        fetch('/api/disk_usage')
            .then(response => response.json())
            .then(data => {
                console.log("Disk Usage Data:", data);
                if (!Array.isArray(data) || data.length === 0) {
                    console.warn("No disk usage data received or data is empty.");
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
                            datalabels: {
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
                    plugins: [ChartDataLabels]
                });
            })
            .catch(error => {
                console.error("Failed to load disk usage data:", error);
                const canvas = document.getElementById('diskUsageChart');
                 if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#dc3545';
                        ctx.textAlign = 'center';
                        ctx.fillText('Error loading disk usage data.', canvas.width / 2, canvas.height / 2);
                    }
            });
    }

    let s3UsageChart = null;
    function initializeS3UsageChart() {
        fetch('/api/s3_usage')
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    const canvas = document.getElementById('s3UsageChart');
                    if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#dc3545';
                        ctx.textAlign = 'center';
                        ctx.fillText(data.error, canvas.width / 2, canvas.height / 2);
                    }
                    return;
                }

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
                const totalUsage = Array(labels.length).fill(0);

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
                        return;
                    }
                    if (!bucket.prefixes) return;

                    bucket.prefixes.forEach(prefix => {
                        const prefixSize = prefix.size_gib || 0;
                        totalUsage[bucketIndex] += prefixSize;
                        const prefixColor = getColor(prefix.prefix);

                        datasets.push({
                            label: prefix.prefix || 'Root',
                            data: labels.map((_, index) => (index === bucketIndex ? prefixSize : 0)),
                            backgroundColor: prefixColor,
                            borderColor: prefixColor.replace('0.6', '1'),
                            borderWidth: 1
                        });

                        if (!prefix.sub_prefixes) return;

                        prefix.sub_prefixes.forEach(sub_prefix => {
                            const subPrefixSize = sub_prefix.size_gib || 0;
                            totalUsage[bucketIndex] += subPrefixSize;
                            const subPrefixColor = getColor(sub_prefix.prefix);

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
                            legend: { display: false },
                            tooltip: {
                                callbacks: {
                                    label: function (context) {
                                        return `${context.dataset.label}: ${context.raw.toFixed(2)} GiB`;
                                    }
                                }
                            },
                            datalabels: {
                                anchor: 'end',
                                align: 'end',
                                formatter: function (value, context) {
                                    const isLastDataset = context.chart.data.datasets
                                        .filter(ds => ds.data[context.dataIndex] > 0)
                                        .slice(-1)[0] === context.dataset;

                                    if (isLastDataset) {
                                        return `${totalUsage[context.dataIndex].toFixed(2)} GiB`;
                                    }
                                    return null;
                                },
                                color: '#fff'
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
                    plugins: [ChartDataLabels]
                });
            })
            .catch(error => {
                console.error("Failed to load S3 usage data:", error);
                 const canvas = document.getElementById('s3UsageChart');
                 if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.font = '16px Arial';
                        ctx.fillStyle = '#dc3545';
                        ctx.textAlign = 'center';
                        ctx.fillText('Error loading S3 usage data.', canvas.width / 2, canvas.height / 2);
                    }
            });
    }

    let scheduledJobsMiniChart = null;
    function initializeScheduledJobsMiniChart() {
        const canvas = document.getElementById('scheduledJobsMiniChart');
        const card = document.getElementById('scheduledJobsMiniChart-card');
        const nodata = document.getElementById('scheduledJobsMiniChart-nodata');
        if (!canvas) return;
        fetch("/data/dashboard/scheduler_events.json")
            .then(r => r.json())
            .then(data => {
                data = data.slice().reverse();

                const barColors = data.map(event => {
                    if (event.job_name === "No jobs" || event.status === "none") {
                        return "rgba(15, 107, 255, 0.47)";
                    }
                    if (event.status === "error") {
                        return "rgba(220, 53, 70, 0.43)";
                    }
                    return "rgba(40, 167, 70, 0.49)";
                });

                if (window.scheduledJobsMiniChart && typeof window.scheduledJobsMiniChart.destroy === "function") {
                    window.scheduledJobsMiniChart.destroy();
                }
                window.scheduledJobsMiniChart = new Chart(canvas.getContext('2d'), {
                    type: 'bar',
                    data: {
                        labels: data.map(e => e.datetime || ""),
                        datasets: [{
                            data: data.map(() => 1),
                            backgroundColor: barColors,
                            borderWidth: 0,
                            barPercentage: 0.99,
                            categoryPercentage: 0.99
                        }]
                    },
                    options: {
                        plugins: {
                            legend: { display: false },
                            tooltip: {
                                displayColors: true,
                                callbacks: {
                                    title: ctx => {
                                        const e = data[ctx[0].dataIndex];
                                        return e.datetime || "";
                                    },
                                    label: ctx => {
                                        const e = data[ctx.dataIndex];
                                        if (e.job_name === "No jobs" || e.status === "none") {
                                            return "No jobs run";
                                        }
                                        return `${e.job_name || "?"} (${e.backup_type || "unknown"}) : ${e.status || "unknown"}`;
                                    }
                                }
                            }
                        },
                        scales: {
                            x: {
                                display: false,
                                grid: { display: false, drawBorder: false },
                                barPercentage: 0.99,
                                categoryPercentage: 0.99
                            },
                            y: {
                                display: false,
                                grid: { display: false, drawBorder: false }
                            }
                        },
                        elements: { bar: { borderRadius: 0 } },
                        responsive: true,
                        maintainAspectRatio: false,
                        animation: false
                    }
                });
            })
            .catch((error) => {
                console.error("Failed to fetch scheduler events:", error);
                if (card) card.style.maxHeight = "50px";
                if (nodata) nodata.textContent = "Error loading scheduler data.";
                if (nodata) nodata.style.display = "";
                if (canvas) canvas.style.display = "none";
            });
}
    
    // Initialize charts
    initializeDiskUsageChart();
    initializeS3UsageChart();
    initializeScheduledJobsMiniChart();

    // Refresh the table data and mini chart periodically
    setInterval(function () {
        eventsTable.ajax.reload(null, false);
        initializeScheduledJobsMiniChart();
    }, 10000); // every 10 seconds

}); // End document ready

// === Monitor Badge Functions ===
function updateMonitorBadges() {
    console.log('updateMonitorBadges called');
    fetch('/api/discovered_instances_with_status')
        .then(response => {
            console.log('API response received:', response);
            return response.json();
        })
        .then(data => {
            console.log('API data:', data);
            if (data.success) {
                updateInstanceBadges(data.instances);
            } else {
                console.error('Failed to load discovered instances:', data.error);
            }
        })
        .catch(error => {
            console.error('Failed to fetch discovered instances:', error);
        });
}

function updateInstanceBadges(instances) {
    console.log('updateInstanceBadges called with:', instances);
    const badgeContainer = document.getElementById('jabs-instance-badges');
    if (!badgeContainer) {
        console.log('Badge container not found!');
        return;
    }
    
    // Clear existing badges
    badgeContainer.innerHTML = '';
    
    // Create badges for each discovered instance
    instances.forEach(instance => {
        console.log('Processing instance:', instance);
        
        const flaskOnline = instance.flask_status === 'online';
        const cliOnline = instance.cli_status === 'online';
        
        // Check for errors in status data
        const flaskErrors = instance.status_data?.flask_data?.error_event_count || 0;
        const cliErrors = instance.status_data?.cli_data?.error_event_count || 0;
        const hasErrors = flaskErrors > 0 || cliErrors > 0;
        
        // Determine status and color based on what's running and error status
        let status, color, tooltip;
        if (hasErrors) {
            status = 'errors';
            color = 'FF0000'; // Red - errors detected
            tooltip = `Errors detected: Flask(${flaskErrors}) CLI(${cliErrors})`;
        } else if (flaskOnline && cliOnline) {
            status = 'online';
            color = '00DF4D'; // Green - both running
            tooltip = 'Both Web and CRON are running';
        } else if (flaskOnline && !cliOnline) {
            status = 'web%20only';
            color = 'FFA500'; // Orange - web only
            tooltip = 'Web interface online, CRON stopped';
        } else if (!flaskOnline && cliOnline) {
            status = 'cron%20only';
            color = 'FFD700'; // Gold - cron only
            tooltip = 'CRON running, Web interface offline';
        } else {
            status = 'offline';
            color = 'FF0000'; // Red - both down
            tooltip = 'Both Web and CRON are offline';
        }
        
        console.log(`Status: ${status}, Color: ${color}, Flask: ${flaskOnline}, CLI: ${cliOnline}`);
        
        // Create badge container (link only if Flask is online)
        const badgeElement = document.createElement(flaskOnline ? 'a' : 'span');
        if (flaskOnline) {
            badgeElement.href = `http://${instance.ip_address}:${instance.port}`;
            badgeElement.target = '_blank';
            badgeElement.style.textDecoration = 'none';
            badgeElement.style.cursor = 'pointer';
        } else {
            badgeElement.style.cursor = 'default';
        }
        
        // Create badge image
        const badgeImg = document.createElement('img');
        const badgeText = instance.hostname || `${instance.ip_address}:${instance.port}`;
        badgeImg.src = `https://img.shields.io/static/v1?label=${encodeURIComponent(badgeText)}&message=${status}&labelColor=0B0021&color=${color}&style=flat&logo=server&logoColor=white`;
        badgeImg.alt = `JABS Server: ${badgeText}`;
        badgeImg.className = 'me-2 mb-2';
        badgeImg.title = `${badgeText} (${instance.version || 'Unknown'})
${tooltip}
Flask: ${instance.flask_status.toUpperCase()}${flaskErrors > 0 ? ` (${flaskErrors} errors)` : ''}
CLI: ${instance.cli_status.toUpperCase()}${cliErrors > 0 ? ` (${cliErrors} errors)` : ''}
Grace Period: ${instance.grace_period_minutes || 60} min${!flaskOnline ? '\n(Click disabled - Web interface offline)' : ''}`;
        badgeImg.style.cssText = 'max-width: 100%; height: 20px;';
        
        badgeElement.appendChild(badgeImg);
        badgeContainer.appendChild(badgeElement);
        console.log('Added badge for:', badgeText, 'Status:', status);
    });
    
    console.log('Finished adding badges, container now has', badgeContainer.children.length, 'children');
}