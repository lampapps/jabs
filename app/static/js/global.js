// filepath: /home/jim2/jabs3/static/js/global.js

// --- Theme Switcher Functions ---
function setTheme(mode) {
  localStorage.setItem('theme', mode);
  applyTheme(mode);
}

function applyTheme(mode) {
  const root = document.documentElement;
  let iconClass = 'fa-circle-half-stroke'; // Default for 'auto'

  if (mode === 'light') {
    root.setAttribute('data-bs-theme', 'light');
    iconClass = 'fa-sun text-warning';
  } else if (mode === 'dark') {
    root.setAttribute('data-bs-theme', 'dark');
    iconClass = 'fa-moon text-primary';
  } else { // 'auto' or invalid
    root.removeAttribute('data-bs-theme'); // Use OS preference via CSS media query
    iconClass = 'fa-circle-half-stroke text-secondary'; // Icon for auto
  }

  // Update both desktop and mobile theme icons
  document.querySelectorAll('#currentThemeIcon, #currentThemeIconMobile').forEach(icon => {
      if (icon) { // Check if icon exists on the page
         icon.className = 'fas me-2 ' + iconClass;
      }
  });
}
// --- End Theme Switcher Functions ---


$(document).ready(function () { // Ensure DOM is ready

    // --- Apply Stored Theme on Load  ---
    const savedTheme = localStorage.getItem('theme') || 'auto';
    applyTheme(savedTheme);
    // --- End Apply Stored Theme ---


    // --- Scheduler Status Check ---
    function updateSchedulerStatus() {
        fetch('/api/scheduler_status')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                let iconClass = 'fas fa-question-circle text-secondary fa-lg'; // Default: unknown
                let title = data.message || 'Status unknown';

                if (data.status === 'ok') {
                    iconClass = 'fas fa-check-circle text-success fa-lg'; // OK: Green check
                } else if (data.status === 'stale') {
                    iconClass = 'fas fa-exclamation-triangle text-warning fa-lg'; // Stale: Yellow warning
                } else if (data.status === 'error') {
                    iconClass = 'fas fa-times-circle text-danger fa-lg'; // Error: Red cross
                }

                // Select both desktop and mobile icons
                const iconSelector = '#schedulerStatusIconDesktop, #schedulerStatusIconMobile';
                const icons = $(iconSelector);

                // Check if icons exist before trying to update
                if (icons.length > 0) {
                    icons.attr('class', iconClass);
                    // Update tooltip (title attribute of the parent span)
                    icons.parent().attr('title', title);
                }
            })
            .catch(error => {
                console.error("Failed to fetch scheduler status:", error);
                const iconSelector = '#schedulerStatusIconDesktop, #schedulerStatusIconMobile';
                const icons = $(iconSelector);
                 if (icons.length > 0) {
                    icons.attr('class', 'fas fa-exclamation-circle text-danger fa-lg');
                    icons.parent().attr('title', 'Failed to fetch status');
                 }
            });
    }

    // Initial check for scheduler status
    updateSchedulerStatus();
    // Periodically check scheduler status
    setInterval(updateSchedulerStatus, 30000); // Check every 30 seconds
    // --- End Scheduler Status Check ---

}); // End document ready