$(document).ready(function () {
    // Extract job_name and backup_set_id from URL for API call
    const urlParts = window.location.pathname.split('/');
    const jobName = decodeURIComponent(urlParts[urlParts.length - 2]);
    const backupSetId = decodeURIComponent(urlParts[urlParts.length - 1]);
    const apiUrl = `/api/manifest/${jobName}/${backupSetId}/json`;

    // Track restore status
    let isRestoreRunning = false;

    // Initialize UI state immediately (before any AJAX calls)
    function initializeUI() {
        // Show/hide path input based on radio button selection
        if ($('#restoreCustom').is(':checked')) {
            $('#customPathCol').show();
            $('#sourcePathCol').hide();
        } else {
            $('#customPathCol').hide();
            $('#sourcePathCol').show();
        }
        
        // Initialize button states - start with buttons enabled
        isRestoreRunning = false;
        $('#restoreProgressContainer').hide();
        updateRestoreButtonStates();
    }

    // Call initialization immediately
    initializeUI();

    // Fetch manifest data for the table and initialize DataTable
    $.getJSON(apiUrl, function (data) {
        $('#manifestTable').DataTable({
            data: data.files || [],
            columns: [
                {
                    data: null,
                    orderable: false,
                    className: 'text-center',
                    render: function (data, type, row, meta) {
                        return `<input type="checkbox" class="file-checkbox" value="${row.path}" data-tarball="${row.tarball_path}">`;
                    }
                },
                { data: 'tarball', title: 'Tarball' },
                { data: 'path', title: 'File' },
                { data: 'size', title: 'Size' },
                { data: 'modified', title: 'Last Modified' }
            ],
            columnDefs: [
                { targets: 3, className: 'text-center' },
                { targets: 4, responsivePriority: 3 },
                { targets: 1, responsivePriority: 2 },
                { targets: 2, responsivePriority: 1 },
                { targets: [0, 3], responsivePriority: 100 },
            ],
            language: {
                search: "Filter files:",
                lengthMenu: "Show _MENU_ files",
                info: "Showing _START_ to _END_ of _TOTAL_ files",
            },
            order: [[1, 'asc']],
            responsive: true,
            paging: true,
            searching: true,
            ordering: true,
            lengthMenu: [25, 50, 100, 200],
            initComplete: function() {
                // Update button states after table is loaded
                updateRestoreButtonStates();
            }
        });
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.error("Failed to load manifest JSON for table:", textStatus, errorThrown);
        $('#manifestTable tbody').html('<tr><td colspan="5" class="text-center text-danger">Failed to load manifest file data.</td></tr>');
    });

    // Toggle custom path or source path input based on selected restore location
    $('input[name="restoreLocation"]').change(function() {
        if ($('#restoreCustom').is(':checked')) {
            $('#customPathCol').show();
            $('#sourcePathCol').hide();
        } else {
            $('#customPathCol').hide();
            $('#sourcePathCol').show();
        }
    });

    let restoreType = "full";
    let selectedFiles = [];

    $('#restoreFullBtn').click(function() {
        if (isRestoreRunning) return;
        restoreType = "full";
        selectedFiles = [];
        startRestore();
    });

    $('#restoreSelectedBtn').click(function() {
        if (isRestoreRunning) return;
        restoreType = "selected";
        selectedFiles = [];
        $('.file-checkbox:checked').each(function() {
            selectedFiles.push({
                path: $(this).val(),
                tarball_path: $(this).data('tarball')
            });
        });
        startRestore();
    });

    function isValidPath(path) {
        // Basic check: not empty, no illegal chars (Linux/Unix)
        return path && !/[<>:"|?*]/.test(path);
    }

    function updateRestoreButtonStates() {
        // Update "Restore Selected" button based on file selection and restore status
        const hasSelectedFiles = $('.file-checkbox:checked').length > 0;
        $('#restoreSelectedBtn').prop('disabled', isRestoreRunning || !hasSelectedFiles);
        
        // Update "Restore Full" button based only on restore status
        $('#restoreFullBtn').prop('disabled', isRestoreRunning);
    }

    function startRestore() {
        const locationType = $('input[name="restoreLocation"]:checked').val();
        const customPath = $('#customRestorePath').val();

        // Custom path validation
        if (locationType === "custom") {
            if (!isValidPath(customPath)) {
                alert("Please enter a valid destination path for custom restore.");
                return;
            }
        }

        // File selection validation for "Restore Selected Files"
        if (restoreType === "selected" && selectedFiles.length === 0) {
            alert("Please select at least one file to restore.");
            return;
        }

        // Overwrite warning for original locations
        if (locationType === "original") {
            if (!confirm("Warning: Restoring to original locations may overwrite existing files. Continue?")) {
                return;
            }
        }

        // Overwrite warning for custom directory
        if (locationType === "custom") {
            if (!confirm("Warning: Files in the destination directory may be overwritten if they already exist. Continue?")) {
                return;
            }
        }

        // Set restore as running and update UI
        isRestoreRunning = true;
        updateRestoreButtonStates();
        $('#restoreProgressContainer').show();

        const payload = {
            job_name: jobName,
            backup_set_id: backupSetId,
            restore_location: locationType,
            custom_path: customPath
        };
        
        if (restoreType === "selected") {
            payload.files = selectedFiles;
            $.ajax({
                url: '/api/restore/files',
                type: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(payload),
                success: handleRestoreResponse,
                error: handleRestoreError
            });
        } else {
            $.ajax({
                url: '/api/restore/full',
                type: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(payload),
                success: handleRestoreResponse,
                error: handleRestoreError
            });
        }
    }

    function handleRestoreResponse(response) {
        if (response.redirect) {
            window.location.href = response.redirect;
        } else {
            window.location.reload();
        }
    }

    function handleRestoreError(xhr) {
        // Reset restore status
        isRestoreRunning = false;
        updateRestoreButtonStates();
        $('#restoreProgressContainer').hide();
        
        let msg = "Restore failed. Please check the logs.";
        if (xhr.responseJSON && xhr.responseJSON.error) {
            msg = xhr.responseJSON.error;
            if (xhr.responseJSON.files) {
                msg += "\nFiles: " + xhr.responseJSON.files.join(', ');
            }
        }
        alert(msg);
    }

    // Enable/disable Restore Selected button based on file selection
    $(document).on('change', '.file-checkbox, #selectAllFiles', function() {
        updateRestoreButtonStates();
    });

    // Select/Deselect all checkboxes
    $(document).on('change', '#selectAllFiles', function() {
        $('.file-checkbox').prop('checked', this.checked).trigger('change');
    });

    function pollRestoreStatus() {
        $.getJSON(`/api/restore/status/${jobName}/${backupSetId}`, function(data) {
            const wasRunning = isRestoreRunning;
            isRestoreRunning = data.running || false;
            
            if (isRestoreRunning) {
                $('#restoreProgressContainer').show();
            } else {
                $('#restoreProgressContainer').hide();
            }
            
            // Update button states
            updateRestoreButtonStates();
            
            // If restore just finished, we might want to reload the page
            if (wasRunning && !isRestoreRunning) {
                // Optional: reload page when restore completes
                // setTimeout(() => window.location.reload(), 1000);
            }
        }).fail(function(jqXHR, textStatus, errorThrown) {
            // If the API call fails, assume no restore is running
            console.warn("Failed to check restore status:", textStatus, errorThrown);
            // Don't change isRestoreRunning here - only change it on successful API calls
            // This prevents buttons from being stuck disabled if the API is temporarily unavailable
            if (!isRestoreRunning) {
                $('#restoreProgressContainer').hide();
                updateRestoreButtonStates();
            }
        });
    }

    // Start polling after a short delay to allow page to fully load
    setTimeout(function() {
        pollRestoreStatus();
        setInterval(pollRestoreStatus, 5000);
    }, 1000);

    // Enable Bootstrap 5 tooltips
    document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function (el) {
        new bootstrap.Tooltip(el);
    });
    
    // Hide spinner when everything is ready
    document.getElementById('loading-spinner').style.display = 'none';
});