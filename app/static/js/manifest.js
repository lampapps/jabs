$(document).ready(function () {
    // Extract job_name and backup_set_id from URL for API call
    const urlParts = window.location.pathname.split('/');
    const jobName = decodeURIComponent(urlParts[urlParts.length - 2]);
    const backupSetId = decodeURIComponent(urlParts[urlParts.length - 1]);
    const apiUrl = `/api/manifest/${jobName}/${backupSetId}/json`;

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
                        return `<input type="checkbox" class="file-checkbox" value="${row.path}">`;
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
            order: [[1, 'asc']],
            responsive: true,
            paging: true,
            searching: true,
            ordering: true,
            lengthMenu: [25, 50, 100, 200]
        });
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.error("Failed to load manifest JSON for table:", textStatus, errorThrown);
        $('#manifestTable tbody').html('<tr><td colspan="5" class="text-center text-danger">Failed to load manifest file data.</td></tr>');
    });

    // Show custom path input if "Custom Directory" is default
    if ($('#restoreCustom').is(':checked')) {
        $('#customPathCol').show();
    }

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

    // On page load, set the correct visibility
    if ($('#restoreCustom').is(':checked')) {
        $('#customPathCol').show();
        $('#sourcePathCol').hide();
    } else {
        $('#customPathCol').hide();
        $('#sourcePathCol').show();
    }

    let restoreType = "full";
    let selectedFiles = [];

    $('#restoreFullBtn').click(function() {
        restoreType = "full";
        selectedFiles = [];
        startRestore();
    });

    $('#restoreSelectedBtn').click(function() {
        restoreType = "selected";
        selectedFiles = [];
        $('.file-checkbox:checked').each(function() {
            selectedFiles.push($(this).val());
        });
        startRestore();
    });

    function isValidPath(path) {
        // Basic check: not empty, no illegal chars (Linux/Unix)
        return path && !/[<>:"|?*]/.test(path);
    }

    function startRestore() {
        const locationType = $('input[name="restoreLocation"]:checked').val();
        const customPath = $('#customRestorePath').val();

        // Custom path validation
        if (locationType === "custom") {
            if (!isValidPath(customPath)) {
                alert("Please enter a valid destination path for custom restore.");
                $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', false);
                $('#restoreProgressContainer').hide();
                $('#customRestorePath').focus();
                return;
            }
        }

        // File selection validation for "Restore Selected Files"
        if (restoreType === "selected" && selectedFiles.length === 0) {
            alert("Please select at least one file to restore.");
            $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', false);
            $('#restoreProgressContainer').hide();
            return;
        }

        // Overwrite warning for original locations
        if (locationType === "original") {
            if (!confirm("Warning: Restoring to original locations may overwrite existing files. Continue?")) {
                $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', false);
                $('#restoreProgressContainer').hide();
                return;
            }
        }

        // Overwrite warning for custom directory
        if (locationType === "custom") {
            if (!confirm("Warning: Files in the destination directory may be overwritten if they already exist. Continue?")) {
                $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', false);
                $('#restoreProgressContainer').hide();
                return;
            }
        }

        // Disable buttons and show progress
        $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', true);
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
        $('#restoreProgressContainer').hide();
        $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', false);
        let msg = "Restore failed. Please check the logs.";
        if (xhr.responseJSON && xhr.responseJSON.error) {
            msg = xhr.responseJSON.error;
            if (xhr.responseJSON.files) {
                msg += "\nFiles: " + xhr.responseJSON.files.join(', ');
            }
        }
        alert(msg);
    }

    // Enable/disable Restore Selected button
    $(document).on('change', '.file-checkbox, #selectAllFiles', function() {
        $('#restoreSelectedBtn').prop('disabled', $('.file-checkbox:checked').length === 0);
    });

    // Select/Deselect all checkboxes
    $(document).on('change', '#selectAllFiles', function() {
        $('.file-checkbox').prop('checked', this.checked).trigger('change');
    });

    function pollRestoreStatus() {
        $.getJSON(`/api/restore/status/${jobName}/${backupSetId}`, function(data) {
            if (data.running) {
                $('#restoreProgressContainer').show();
                $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', true);
            } else {
                $('#restoreProgressContainer').hide();
                $('#restoreFullBtn, #restoreSelectedBtn').prop('disabled', false);
            }
        });
    }

    // Poll on page load and every 5 seconds
    pollRestoreStatus();
    setInterval(pollRestoreStatus, 5000);

    // Enable Bootstrap 5 tooltips
    document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function (el) {
        new bootstrap.Tooltip(el);
    });
});