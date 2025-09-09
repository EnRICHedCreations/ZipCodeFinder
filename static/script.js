// Global variables
let currentSessionId = null;
let processingInterval = null;
let isProcessing = false;

// DOM elements
const uploadArea = document.getElementById('uploadArea');
const fileInput = document.getElementById('fileInput');
const configSection = document.getElementById('configSection');
const progressSection = document.getElementById('progressSection');
const resultsSection = document.getElementById('resultsSection');
const processBtn = document.getElementById('processBtn');
const stopBtn = document.getElementById('stopBtn');
const downloadSection = document.getElementById('downloadSection');

// Column selects
const addressColumn = document.getElementById('addressColumn');
const cityColumn = document.getElementById('cityColumn');
const stateColumn = document.getElementById('stateColumn');
const zipColumn = document.getElementById('zipColumn');

// Progress elements
const progressFill = document.getElementById('progressFill');
const progressText = document.getElementById('progressText');
const processedCount = document.getElementById('processedCount');
const successCount = document.getElementById('successCount');
const failedCount = document.getElementById('failedCount');
const successRate = document.getElementById('successRate');
const resultsLog = document.getElementById('resultsLog');

// Initialize drag and drop
function initializeDragDrop() {
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('dragleave', handleDragLeave);
    uploadArea.addEventListener('drop', handleDrop);
    uploadArea.addEventListener('click', () => fileInput.click());
    fileInput.addEventListener('change', handleFileSelect);
}

function handleDragOver(e) {
    e.preventDefault();
    uploadArea.classList.add('dragover');
}

function handleDragLeave(e) {
    e.preventDefault();
    uploadArea.classList.remove('dragover');
}

function handleDrop(e) {
    e.preventDefault();
    uploadArea.classList.remove('dragover');
    
    const files = e.dataTransfer.files;
    if (files.length > 0) {
        handleFileUpload(files[0]);
    }
}

function handleFileSelect(e) {
    const file = e.target.files[0];
    if (file) {
        handleFileUpload(file);
    }
}

async function handleFileUpload(file) {
    if (!file.name.toLowerCase().endsWith('.csv')) {
        showError('Please select a CSV file.');
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
        showLoading('Uploading file...');
        
        const response = await fetch('/upload', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();
        
        hideLoading();

        if (response.ok) {
            currentSessionId = result.session_id;
            populateColumnSelects(result.headers);
            showConfigSection(result.filename);
        } else {
            showError(result.error || 'Upload failed');
        }
    } catch (error) {
        hideLoading();
        showError('Upload failed: ' + error.message);
    }
}

function populateColumnSelects(headers) {
    // Clear existing options
    [addressColumn, cityColumn, stateColumn, zipColumn].forEach(select => {
        select.innerHTML = '';
    });

    // Add headers as options
    headers.forEach(header => {
        [addressColumn, cityColumn, stateColumn, zipColumn].forEach(select => {
            const option = document.createElement('option');
            option.value = header;
            option.textContent = header;
            select.appendChild(option);
        });
    });

    // Auto-select common column names
    autoSelectColumns(headers);
}

function autoSelectColumns(headers) {
    const patterns = {
        address: ['address', 'street', 'property address', 'property_address'],
        city: ['city', 'town', 'municipality'],
        state: ['state', 'st', 'province'],
        zip: ['zip', 'zipcode', 'zip code', 'postal', 'postal code']
    };

    const headersLower = headers.map(h => h.toLowerCase());

    // Find and select matching columns
    for (const [type, patternList] of Object.entries(patterns)) {
        for (const pattern of patternList) {
            const index = headersLower.indexOf(pattern);
            if (index !== -1) {
                const select = document.getElementById(type + 'Column');
                if (select) {
                    select.value = headers[index];
                    break;
                }
            }
        }
    }
}

function showConfigSection(filename) {
    uploadArea.querySelector('p').textContent = `Selected: ${filename}`;
    configSection.style.display = 'block';
    configSection.classList.add('fade-in');
}

async function processFile() {
    if (!currentSessionId) {
        showError('No file uploaded');
        return;
    }

    const config = {
        session_id: currentSessionId,
        address_column: addressColumn.value,
        city_column: cityColumn.value,
        state_column: stateColumn.value,
        zip_column: zipColumn.value
    };

    if (!config.address_column || !config.city_column || !config.state_column || !config.zip_column) {
        showError('Please select all column mappings');
        return;
    }

    try {
        const response = await fetch('/process', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });

        const result = await response.json();

        if (response.ok) {
            startProcessing();
        } else {
            showError(result.error || 'Processing failed to start');
        }
    } catch (error) {
        showError('Processing failed: ' + error.message);
    }
}

function startProcessing() {
    isProcessing = true;
    processBtn.style.display = 'none';
    stopBtn.style.display = 'inline-block';
    
    showProgressSection();
    showResultsSection();
    
    // Start polling for progress
    processingInterval = setInterval(updateProgress, 1000);
}

async function stopProcessing() {
    if (!currentSessionId) return;

    try {
        const response = await fetch(`/stop/${currentSessionId}`, {
            method: 'POST'
        });

        if (response.ok) {
            clearInterval(processingInterval);
            isProcessing = false;
            processBtn.style.display = 'inline-block';
            stopBtn.style.display = 'none';
            progressText.textContent = 'Processing stopped by user';
        }
    } catch (error) {
        showError('Failed to stop processing: ' + error.message);
    }
}

async function updateProgress() {
    if (!currentSessionId || !isProcessing) return;

    try {
        const response = await fetch(`/status/${currentSessionId}`);
        const status = await response.json();

        if (response.ok) {
            updateProgressDisplay(status);
            updateResultsLog(status.results_log);

            if (status.processing_complete) {
                completeProcessing();
            }
        }
    } catch (error) {
        console.error('Failed to update progress:', error);
    }
}

function updateProgressDisplay(status) {
    const progress = status.progress_percent || 0;
    progressFill.style.width = progress + '%';
    
    if (status.current_address) {
        progressText.textContent = `Processing: ${status.current_address}`;
    } else {
        progressText.textContent = `Progress: ${progress}%`;
    }

    processedCount.textContent = status.total_processed || 0;
    successCount.textContent = status.successful_geocodes || 0;
    failedCount.textContent = status.failed_geocodes || 0;

    const total = (status.successful_geocodes || 0) + (status.failed_geocodes || 0);
    const rate = total > 0 ? Math.round((status.successful_geocodes / total) * 100) : 0;
    successRate.textContent = rate + '%';
}

function updateResultsLog(logEntries) {
    if (!logEntries || logEntries.length === 0) return;

    // Only show the most recent entries to avoid overwhelming the UI
    const recentEntries = logEntries.slice(-50);
    
    resultsLog.innerHTML = '';
    recentEntries.forEach(entry => {
        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${entry.type}`;
        logEntry.innerHTML = `
            <span class="log-timestamp">${entry.timestamp}</span>
            ${entry.message}
        `;
        resultsLog.appendChild(logEntry);
    });

    // Scroll to bottom
    resultsLog.scrollTop = resultsLog.scrollHeight;
}

function completeProcessing() {
    clearInterval(processingInterval);
    isProcessing = false;
    processBtn.style.display = 'inline-block';
    stopBtn.style.display = 'none';
    
    progressText.textContent = 'Processing complete!';
    progressFill.style.width = '100%';
    
    showDownloadSection();
}

function showProgressSection() {
    progressSection.style.display = 'block';
    progressSection.classList.add('slide-up');
}

function showResultsSection() {
    resultsSection.style.display = 'block';
    resultsSection.classList.add('slide-up');
}

function showDownloadSection() {
    downloadSection.style.display = 'block';
    
    const downloadBtn = document.getElementById('downloadBtn');
    downloadBtn.onclick = () => {
        window.location.href = `/download/${currentSessionId}`;
    };
}

function showError(message) {
    alert('Error: ' + message);
}

function showLoading(message) {
    progressText.innerHTML = `<div class="loading"></div>${message}`;
    progressSection.style.display = 'block';
}

function hideLoading() {
    if (!isProcessing) {
        progressSection.style.display = 'none';
    }
}

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    initializeDragDrop();
    
    // Add click handler for process button
    processBtn.addEventListener('click', processFile);
    
    // Handle page unload
    window.addEventListener('beforeunload', function(e) {
        if (isProcessing) {
            e.preventDefault();
            e.returnValue = 'Processing is still running. Are you sure you want to leave?';
        }
    });
});

// Utility functions
function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function formatDuration(seconds) {
    if (seconds < 60) {
        return `${seconds}s`;
    } else if (seconds < 3600) {
        return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    } else {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        return `${hours}h ${minutes}m`;
    }
}