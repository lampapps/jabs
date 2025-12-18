// Monitor page JavaScript

$(document).ready(function() {
    // Load discovered instances immediately
    refreshDiscoveredInstances();
});

function discoverInstances() {
    const discoverBtn = document.getElementById('discover-btn');
    const statusDiv = document.getElementById('discovery-status');
    const messageSpan = document.getElementById('discovery-message');
    
    // Show discovery status
    statusDiv.classList.add('show');
    discoverBtn.disabled = true;
    discoverBtn.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> Discovering...';
    
    fetch('/api/discover_instances', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            messageSpan.textContent = `Discovery started! Scanning ${data.ip_range_start} to ${data.ip_range_end} on port ${data.port}...`;
            
            // Wait a bit then start checking for results
            setTimeout(() => {
                checkDiscoveryProgress();
            }, 3000);
        } else {
            showDiscoveryError(data.error || 'Discovery failed');
        }
    })
    .catch(error => {
        showDiscoveryError('Network error: ' + error.message);
    });
}

function checkDiscoveryProgress() {
    const statusDiv = document.getElementById('discovery-status');
    const messageSpan = document.getElementById('discovery-message');
    
    // Check for new instances every few seconds
    let attempts = 0;
    const maxAttempts = 20; // Check for up to 60 seconds
    
    const intervalId = setInterval(() => {
        attempts++;
        refreshDiscoveredInstances();
        
        if (attempts >= maxAttempts) {
            clearInterval(intervalId);
            hideDiscoveryStatus();
        }
    }, 3000);
    
    // Hide status after 60 seconds regardless
    setTimeout(() => {
        clearInterval(intervalId);
        hideDiscoveryStatus();
    }, 60000);
}

function showDiscoveryError(error) {
    const statusDiv = document.getElementById('discovery-status');
    const messageSpan = document.getElementById('discovery-message');
    
    statusDiv.className = 'alert alert-danger discovery-status show';
    messageSpan.textContent = 'Discovery error: ' + error;
    
    setTimeout(hideDiscoveryStatus, 5000);
}

function hideDiscoveryStatus() {
    const discoverBtn = document.getElementById('discover-btn');
    const statusDiv = document.getElementById('discovery-status');
    
    statusDiv.classList.remove('show');
    discoverBtn.disabled = false;
    discoverBtn.innerHTML = '<i class="fa fa-search"></i> Auto-Discover';
}

function refreshDiscoveredInstances() {
    console.log('Refresh button clicked - starting refresh...');
    
    // Add visual feedback
    const refreshBtn = document.querySelector('button[onclick="refreshDiscoveredInstances()"]');
    const originalText = refreshBtn.innerHTML;
    refreshBtn.innerHTML = '<i class="fa fa-spinner fa-spin"></i> Refreshing...';
    refreshBtn.disabled = true;
    
    fetch('/api/discovered_instances_with_status')
        .then(response => {
            console.log('API response received:', response.status);
            return response.json();
        })
        .then(data => {
            console.log('Data parsed:', data);
            if (data.success) {
                updateDiscoveredInstancesTable(data.instances);
                console.log('Table updated successfully');
            } else {
                console.error('Failed to load discovered instances:', data.error);
            }
        })
        .catch(error => {
            console.error('Error loading discovered instances:', error);
        })
        .finally(() => {
            // Restore button state
            refreshBtn.innerHTML = originalText;
            refreshBtn.disabled = false;
        });
}

function updateDiscoveredInstancesTable(instances) {
    const tbody = document.getElementById('discovered-instances-tbody');
    
    if (instances.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" class="text-center text-muted">No JABS instances discovered yet. Click "Auto-Discover" to scan your network.</td></tr>';
        return;
    }
    
    let html = '';
    instances.forEach(instance => {
        // Calculate CLI (CRON) status
        const cliLastSeen = instance.cli_last_seen ? new Date(instance.cli_last_seen) : null;
        const now = new Date();
        let cliTimeDiff = cliLastSeen ? (now - cliLastSeen) / (1000 * 60) : null; // minutes ago
        const gracePeriod = instance.grace_period_minutes || 15;
        
        const cliStatusBadge = instance.cli_status === 'online' ? 
            '<span class="badge bg-success">Running</span>' : 
            '<span class="badge bg-danger">Stopped</span>';
            
        const flaskStatusBadge = instance.flask_status === 'online' ? 
            '<span class="badge bg-success">Online</span>' : 
            '<span class="badge bg-danger">Offline</span>';
        
        // Show CLI error count
        let cliErrorCount = 0;
        if (instance.status_data && instance.status_data.cli_data) {
            cliErrorCount = instance.status_data.cli_data.error_event_count || 0;
        }
        
        const cliErrorsDisplay = cliErrorCount > 0 ? 
            `<span class="badge bg-warning">${cliErrorCount}</span>` : 
            '<span class="text-muted">0</span>';
        
        // Show Flask error count (if available in flask_data)
        let flaskErrorCount = 0;
        if (instance.status_data && instance.status_data.flask_data) {
            flaskErrorCount = instance.status_data.flask_data.error_event_count || 0;
        }
        
        const flaskErrorsDisplay = flaskErrorCount > 0 ? 
            `<span class="badge bg-warning">${flaskErrorCount}</span>` : 
            '<span class="text-muted">0</span>';
        
        // Only show Open button if Flask is online
        const flaskIsOnline = instance.flask_status === 'online';
        
        html += `
            <tr>
                <td>
                    <strong>${escapeHtml(instance.hostname)}</strong>
                </td>
                <td>
                    <code>${instance.ip_address}:${instance.port}</code>
                </td>
                <td>
                    <span class="text-muted">${escapeHtml(instance.version || 'Unknown')}</span>
                </td>
                <!-- CLI Status Group -->
                <td class="group-border-cli-start">
                    ${cliStatusBadge}
                </td>
                <td>
                    ${cliErrorsDisplay}
                </td>
                <td class="group-border-cli-end">
                    <div class="d-flex align-items-center">
                        <span class="me-2">${instance.grace_period_minutes}m</span>
                        <button class="btn btn-outline-info btn-sm" 
                                onclick="editGracePeriod(${instance.id}, ${instance.grace_period_minutes})"
                                title="Edit Grace Period">
                            <i class="fa fa-edit"></i>
                        </button>
                    </div>
                </td>
                <!-- Flask Status Group -->
                <td class="group-border-flask-start">
                    ${flaskStatusBadge}
                </td>
                <td>
                    ${flaskErrorsDisplay}
                </td>
                <td class="group-border-flask-end">
                    ${flaskIsOnline ? `
                        <a href="${instance.url}" target="_blank" class="btn btn-primary btn-sm">
                            <i class="fa fa-external-link"></i> Open
                        </a>` : '<span class="text-muted">N/A</span>'}
                </td>
                <!-- Actions Group -->
                <td class="group-border-actions">
                    <button class="btn btn-outline-danger btn-sm" 
                            onclick="deleteInstance(${instance.id})"
                            title="Delete">
                        <i class="fa fa-trash"></i>
                    </button>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html;
}

function deleteInstance(instanceId) {
    if (!confirm('Are you sure you want to delete this discovered instance?')) {
        return;
    }
    
    fetch(`/api/discovered_instances/${instanceId}`, {
        method: 'DELETE'
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            refreshDiscoveredInstances(); // Refresh the table
        } else {
            alert('Failed to delete instance: ' + data.error);
        }
    })
    .catch(error => {
        alert('Error deleting instance: ' + error.message);
    });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

let currentEditInstanceId = null;

function editGracePeriod(instanceId, currentGracePeriod) {
    currentEditInstanceId = instanceId;
    document.getElementById('gracePeriodInput').value = currentGracePeriod;
    
    const modal = new bootstrap.Modal(document.getElementById('gracePeriodModal'));
    modal.show();
}

function saveGracePeriod() {
    const gracePeriod = parseInt(document.getElementById('gracePeriodInput').value);
    
    if (!gracePeriod || gracePeriod < 1 || gracePeriod > 1440) {
        alert('Please enter a valid grace period between 1 and 1440 minutes.');
        return;
    }
    
    fetch(`/api/discovered_instances/${currentEditInstanceId}/grace_period`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            grace_period_minutes: gracePeriod
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            const modal = bootstrap.Modal.getInstance(document.getElementById('gracePeriodModal'));
            modal.hide();
            refreshDiscoveredInstances(); // Refresh the table
        } else {
            alert('Failed to update grace period: ' + data.error);
        }
    })
    .catch(error => {
        alert('Error updating grace period: ' + error.message);
    });
}