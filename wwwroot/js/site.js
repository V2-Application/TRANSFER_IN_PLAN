// Initialize DataTables with common configuration
$(document).ready(function () {
    // Initialize all DataTables with responsive features
    initializeDataTables();

    // Initialize Bootstrap tooltips
    initializeTooltips();

    // Initialize sidebar collapse functionality
    initializeSidebarToggle();
});

function initializeDataTables() {
    // DataTables default configuration
    $.fn.dataTable.ext.classes.sPageButton = "btn btn-sm btn-outline-primary";

    // Find all DataTables and initialize them
    $('table[id*="Table"]').each(function () {
        if (!$.fn.DataTable.isDataTable(this)) {
            var $table = $(this);
            var config = {
                pageLength: 25,
                responsive: true,
                language: {
                    search: "Search:",
                    lengthMenu: "Show _MENU_ entries",
                    info: "Showing _START_ to _END_ of _TOTAL_ entries",
                    paginate: {
                        first: "First",
                        last: "Last",
                        next: "Next",
                        previous: "Previous"
                    },
                    emptyTable: "No data available"
                },
                dom: '<"top"f>rt<"bottom"lpi><"clear">'
            };

            // Add search and pagination controls
            $table.before('<div class="dataTables_controls d-flex justify-content-between mb-3"></div>');

            $table.DataTable(config);
        }
    });
}

function initializeTooltips() {
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function initializeSidebarToggle() {
    // Handle sidebar collapse links
    var sidebarLinks = document.querySelectorAll('.sidebar .nav-link[href^="#"]');
    sidebarLinks.forEach(function (link) {
        link.addEventListener('click', function (e) {
            e.preventDefault();
            var target = this.getAttribute('href');
            var collapseElement = document.querySelector(target);
            if (collapseElement) {
                var collapse = new bootstrap.Collapse(collapseElement, {
                    toggle: true
                });
            }
        });
    });
}

// Utility functions

function showLoadingSpinner(message = 'Loading...') {
    var html = `
        <div id="loadingOverlay" class="position-fixed top-0 start-0 w-100 h-100 bg-dark bg-opacity-50 d-flex align-items-center justify-content-center" style="z-index: 9999;">
            <div class="text-white text-center">
                <div class="spinner-border mb-3" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p>${message}</p>
            </div>
        </div>
    `;
    $('body').append(html);
}

function hideLoadingSpinner() {
    $('#loadingOverlay').remove();
}

function showAlert(message, type = 'info') {
    var alertClass = 'alert-' + type;
    var icon = type === 'success' ? 'bi-check-circle' :
               type === 'danger' ? 'bi-exclamation-circle' :
               type === 'warning' ? 'bi-exclamation-triangle' :
               'bi-info-circle';

    var html = `
        <div class="alert ${alertClass} alert-dismissible fade show" role="alert" style="margin-top: 20px;">
            <i class="bi ${icon}"></i> ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    $('.main-content').prepend(html);
}

function validateFormInputs(formId) {
    var form = document.getElementById(formId);
    var isValid = true;

    var inputs = form.querySelectorAll('[required]');
    inputs.forEach(function (input) {
        if (!input.value.trim()) {
            input.classList.add('is-invalid');
            isValid = false;
        } else {
            input.classList.remove('is-invalid');
        }
    });

    return isValid;
}

// Export to Excel functionality
function exportTableToExcel(tableId, filename = 'export.xlsx') {
    var table = document.getElementById(tableId);
    var html = table.outerHTML;

    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/Plan/ExportExcel', true);
    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    xhr.responseType = 'blob';

    xhr.onload = function () {
        if (xhr.status === 200) {
            var blob = new Blob([xhr.response], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
            var link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = filename;
            link.click();
        } else {
            showAlert('Error exporting file', 'danger');
        }
    };

    xhr.send();
}

// Format currency
function formatCurrency(value) {
    if (isNaN(value)) return value;
    return parseFloat(value).toLocaleString('en-US', {
        style: 'currency',
        currency: 'USD'
    });
}

// Format number
function formatNumber(value, decimals = 2) {
    if (isNaN(value)) return value;
    return parseFloat(value).toFixed(decimals);
}

// Format date
function formatDate(dateString) {
    var date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    });
}

// Clear form
function clearForm(formId) {
    document.getElementById(formId).reset();
}

// Confirm action
function confirmAction(message = 'Are you sure you want to proceed?') {
    return confirm(message);
}

// Async API call wrapper
async function apiCall(url, method = 'GET', data = null) {
    try {
        showLoadingSpinner();

        var options = {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            }
        };

        if (data) {
            options.body = JSON.stringify(data);
        }

        var response = await fetch(url, options);
        var result = await response.json();

        hideLoadingSpinner();
        return result;
    } catch (error) {
        hideLoadingSpinner();
        showAlert('An error occurred: ' + error.message, 'danger');
        console.error(error);
        return null;
    }
}

// Real-time form validation
document.addEventListener('DOMContentLoaded', function () {
    var forms = document.querySelectorAll('form');
    forms.forEach(function (form) {
        var inputs = form.querySelectorAll('[required]');
        inputs.forEach(function (input) {
            input.addEventListener('change', function () {
                if (this.value.trim()) {
                    this.classList.remove('is-invalid');
                }
            });
        });
    });
});

// Handle form submission with loading indicator
document.addEventListener('DOMContentLoaded', function () {
    var forms = document.querySelectorAll('form[method="post"]');
    forms.forEach(function (form) {
        form.addEventListener('submit', function (e) {
            var submitBtn = form.querySelector('button[type="submit"]');
            if (submitBtn) {
                submitBtn.disabled = true;
                var originalText = submitBtn.innerHTML;
                submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Processing...';
            }
        });
    });
});

// Initialize Popovers
document.addEventListener('DOMContentLoaded', function () {
    var popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });
});
