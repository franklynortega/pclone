let currentUser = null;
let taskId = null;
let pollingInterval = null;

// Elementos DOM
const loginSection = document.getElementById('login-section');
const configSection = document.getElementById('config-section');
const statusSection = document.getElementById('status-section');
const loginForm = document.getElementById('login-form');
const presetSelect = document.getElementById('preset-select');
const loadPresetBtn = document.getElementById('load-preset-btn');
const savePresetBtn = document.getElementById('save-preset-btn');
const executeBtn = document.getElementById('execute-btn');
const backBtn = document.getElementById('back-btn');
const addTableBtn = document.getElementById('add-table-btn');
const loadDefaultTablesBtn = document.getElementById('load-default-tables-btn');
const tablesContainer = document.getElementById('tables-container');
const executeAllPresetsBtn = document.getElementById('execute-all-presets-btn');
const confirmExecuteAllBtn = document.getElementById('confirm-execute-all-btn');
const schedulePresetBtn = document.getElementById('schedule-preset-btn');
const confirmScheduleBtn = document.getElementById('confirm-schedule-btn');
const presetsList = document.getElementById('presets-list');
const schedulesList = document.getElementById('schedules-list');
const configTab = document.getElementById('config-tab');
const presetsTab = document.getElementById('presets-tab');

// Inicialización
document.addEventListener('DOMContentLoaded', () => {
    loginForm.addEventListener('submit', handleLogin);
    loadPresetBtn.addEventListener('click', loadSelectedPreset);
    savePresetBtn.addEventListener('click', savePreset);
    executeBtn.addEventListener('click', executeTask);
    backBtn.addEventListener('click', showConfig);
    addTableBtn.addEventListener('click', addTableEntry);
    loadDefaultTablesBtn.addEventListener('click', loadDefaultTables);
    executeAllPresetsBtn.addEventListener('click', executeAllPresets);
    confirmExecuteAllBtn.addEventListener('click', executeAllPresets);
    schedulePresetBtn.addEventListener('click', showScheduleModal);
    confirmScheduleBtn.addEventListener('click', schedulePreset);

    // Cargar presets al inicio
    loadPresets();

    // Event delegation para remover tablas
    tablesContainer.addEventListener('click', (e) => {
        if (e.target.classList.contains('remove-table-btn')) {
            e.target.closest('.table-entry').remove();
        }
    });
});

// Funciones de autenticación
async function handleLogin(e) {
    e.preventDefault();
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    try {
        const response = await fetch('/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();
        if (data.success) {
            currentUser = data.username;
            showConfig();
            loadPresets();
        } else {
            showError('login-error', data.error);
        }
    } catch (error) {
        showError('login-error', 'Error de conexión');
    }
}

// Funciones de presets
async function loadPresets() {
    try {
        const response = await fetch('/presets', {
            headers: { 'x-username': currentUser }
        });

        if (!response.ok) {
            console.error('Error fetching presets:', response.status);
            return;
        }

        const data = await response.json();
        presetSelect.innerHTML = '<option value="">Seleccionar preset...</option>';
        if (data.presets) {
            data.presets.forEach(preset => {
                const option = document.createElement('option');
                option.value = preset;
                option.textContent = preset;
                presetSelect.appendChild(option);
            });
        }

        // Renderizar lista de presets
        renderPresetsList(data.presets || []);

        // Cargar schedules
        loadSchedules();

        // Si hay presets, mostrar tab Presets por defecto
        if ((data.presets || []).length > 0) {
            const tab = new bootstrap.Tab(presetsTab);
            tab.show();
        }
    } catch (error) {
        console.error('Error cargando presets:', error);
    }
}

async function loadSelectedPreset() {
    const presetName = presetSelect.value;
    if (!presetName) return;

    try {
        const response = await fetch(`/presets/${presetName}`, {
            headers: { 'x-username': currentUser }
        });

        const data = await response.json();
        loadConfig(data.config);
    } catch (error) {
        showError('config-error', 'Error cargando preset');
    }
}

async function savePreset() {
    const presetName = document.getElementById('preset-name').value.trim();
    if (!presetName) {
        showError('config-error', 'Ingrese un nombre para el preset');
        return;
    }

    const config = getConfig();
    if (!config) return;

    try {
        const response = await fetch('/presets', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-username': currentUser
            },
            body: JSON.stringify({ name: presetName, config })
        });

        const data = await response.json();
        if (data.success) {
            alert('Preset guardado exitosamente');
            loadPresets();
            document.getElementById('preset-name').value = '';
        } else {
            showError('config-error', data.error);
        }
    } catch (error) {
        showError('config-error', 'Error guardando preset');
    }
}

// Funciones de configuración
function getConfig() {
    const config = {
        targetServer: document.getElementById('target-server').value.trim(),
        targetDb: document.getElementById('target-db').value.trim(),
        targetUser: document.getElementById('target-user').value.trim(),
        targetPass: document.getElementById('target-pass').value.trim(),
        cloneServer: document.getElementById('clone-server').value.trim(),
        cloneDb: document.getElementById('clone-db').value.trim(),
        cloneUser: document.getElementById('clone-user').value.trim(),
        clonePass: document.getElementById('clone-pass').value.trim(),
        tables: [],
        sync: document.getElementById('sync-checkbox').checked,
        apiKey: document.getElementById('api-key').value.trim()
    };

    // Validar campos requeridos
    const requiredFields = ['targetServer', 'targetDb', 'targetUser', 'targetPass',
                           'cloneServer', 'cloneDb', 'cloneUser', 'clonePass', 'apiKey'];
    for (const field of requiredFields) {
        if (!config[field]) {
            showError('config-error', `Campo ${field} es requerido`);
            return null;
        }
    }

    // Obtener tablas
    const tableEntries = tablesContainer.querySelectorAll('.table-entry');
    for (const entry of tableEntries) {
        const tableName = entry.querySelector('.table-name').value.trim();
        const tablePkStr = entry.querySelector('.table-pk').value.trim();

        if (!tableName || !tablePkStr) {
            showError('config-error', 'Todos los campos de tabla son requeridos');
            return null;
        }

        const pk = tablePkStr.split(',').map(p => p.trim()).filter(p => p);
        if (pk.length === 0) {
            showError('config-error', 'Debe especificar al menos una clave primaria');
            return null;
        }

        config.tables.push({ table: tableName, pk });
    }

    if (config.tables.length === 0) {
        showError('config-error', 'Debe agregar al menos una tabla');
        return null;
    }

    return config;
}

function loadConfig(config) {
    document.getElementById('target-server').value = config.targetServer || '';
    document.getElementById('target-db').value = config.targetDb || '';
    document.getElementById('target-user').value = config.targetUser || '';
    document.getElementById('target-pass').value = config.targetPass || '';
    document.getElementById('clone-server').value = config.cloneServer || '';
    document.getElementById('clone-db').value = config.cloneDb || '';
    document.getElementById('clone-user').value = config.cloneUser || '';
    document.getElementById('clone-pass').value = config.clonePass || '';
    document.getElementById('sync-checkbox').checked = config.sync || false;
    document.getElementById('api-key').value = config.apiKey || '';

    // Limpiar tablas existentes
    tablesContainer.innerHTML = '';

    // Agregar tablas
    config.tables.forEach(table => {
        addTableEntry();
        const entries = tablesContainer.querySelectorAll('.table-entry');
        const lastEntry = entries[entries.length - 1];
        lastEntry.querySelector('.table-name').value = table.table;
        lastEntry.querySelector('.table-pk').value = table.pk.join(', ');
    });
}

function addTableEntry() {
    const entry = document.createElement('div');
    entry.className = 'table-entry mb-2';
    entry.innerHTML = `
        <label class="form-label table-label">Nombre de tabla:</label>
        <input type="text" class="form-control table-name" required>
        <label class="form-label table-label">Claves primarias:</label>
        <input type="text" class="form-control table-pk" placeholder="Separadas por coma" required>
        <button class="btn btn-danger remove-table-btn">Remover</button>
    `;
    tablesContainer.appendChild(entry);
}

async function loadDefaultTables() {
    try {
        const response = await fetch('/tables');
        const data = await response.json();

        // Limpiar tablas existentes
        tablesContainer.innerHTML = '';

        // Agregar tablas por defecto
        data.tables.forEach(table => {
            addTableEntry();
            const entries = tablesContainer.querySelectorAll('.table-entry');
            const lastEntry = entries[entries.length - 1];
            lastEntry.querySelector('.table-name').value = table.table;
            lastEntry.querySelector('.table-pk').value = table.pk.join(', ');
        });
    } catch (error) {
        showError('config-error', 'Error cargando tablas por defecto');
    }
}

// Funciones de ejecución
async function executeTask() {
    const config = getConfig();
    if (!config) return;

    try {
        const response = await fetch('/api/sync', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': config.apiKey
            },
            body: JSON.stringify({
                targetServer: config.targetServer,
                targetDb: config.targetDb,
                targetUser: config.targetUser,
                targetPass: config.targetPass,
                cloneServer: config.cloneServer,
                cloneDb: config.cloneDb,
                cloneUser: config.cloneUser,
                clonePass: config.clonePass,
                tables: config.tables,
                sync: config.sync
            })
        });

        const data = await response.json();
        if (response.ok) {
            taskId = data.taskId;
            showStatus();
            startPolling();
        } else {
            showError('config-error', data.error || 'Error iniciando tarea');
        }
    } catch (error) {
        showError('config-error', 'Error de conexión');
    }
}

function startPolling() {
    if (pollingInterval) clearInterval(pollingInterval);
    pollingInterval = setInterval(pollStatus, 2000);
}

async function pollStatus() {
    if (!taskId) return;

    try {
        const config = getConfig();
        const response = await fetch(`/api/sync/${taskId}`, {
            headers: { 'x-api-key': config.apiKey }
        });

        const data = await response.json();
        updateStatus(data);

        if (data.status === 'completed' || data.status === 'failed') {
            clearInterval(pollingInterval);
            pollingInterval = null;
        }
    } catch (error) {
        console.error('Error obteniendo estado:', error);
    }
}

function updateStatus(data) {
    const statusDiv = document.getElementById('task-status');
    statusDiv.textContent = `Estado: ${data.status}`;
    statusDiv.className = `status-${data.status}`;

    const logsDiv = document.getElementById('task-logs');
    logsDiv.innerHTML = '';

    if (data.logs) {
        data.logs.forEach(log => {
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry log-info';
            logEntry.textContent = log;
            logsDiv.appendChild(logEntry);
        });
    }

    if (data.results) {
        data.results.forEach(result => {
            const resultEntry = document.createElement('div');
            resultEntry.className = `log-entry ${result.status === 'success' ? 'log-success' : 'log-error'}`;
            resultEntry.textContent = `${result.table}: ${result.message}`;
            logsDiv.appendChild(resultEntry);
        });
    }
}

function startInlinePolling() {
    if (pollingInterval) clearInterval(pollingInterval);
    pollingInterval = setInterval(pollInlineStatus, 2000);
}

async function pollInlineStatus() {
    if (!taskId || !currentApiKey) return;

    try {
        const response = await fetch(`/api/sync/${taskId}`, {
            headers: { 'x-api-key': currentApiKey }
        });

        const data = await response.json();
        updateInlineStatus(data);

        if (data.status === 'completed' || data.status === 'failed') {
            clearInterval(pollingInterval);
            pollingInterval = null;
            currentApiKey = null;
        }
    } catch (error) {
        console.error('Error obteniendo estado:', error);
    }
}

function updateInlineStatus(data) {
    const statusDiv = document.getElementById('execution-task-status');
    statusDiv.textContent = `Estado: ${data.status}`;
    statusDiv.className = `status-${data.status}`;

    const logsDiv = document.getElementById('execution-logs');
    logsDiv.innerHTML = '';

    if (data.logs) {
        data.logs.forEach(log => {
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry log-info';
            logEntry.textContent = log;
            logsDiv.appendChild(logEntry);
        });
    }

    if (data.results) {
        data.results.forEach(result => {
            const resultEntry = document.createElement('div');
            resultEntry.className = `log-entry ${result.status === 'success' ? 'log-success' : 'log-error'}`;
            resultEntry.textContent = `${result.table}: ${result.message}`;
            logsDiv.appendChild(resultEntry);
        });
    }

    // Auto-scroll to bottom
    logsDiv.scrollTop = logsDiv.scrollHeight;
}

function showInlineError(message) {
    const logsDiv = document.getElementById('execution-logs');
    logsDiv.innerHTML = `<div class="log-entry log-error">${message}</div>`;
    document.getElementById('execution-status').style.display = 'block';
    document.getElementById('execution-task-status').textContent = 'Estado: Error';
    currentApiKey = null;
}

// Funciones de UI
function showConfig() {
    loginSection.style.display = 'none';
    configSection.style.display = 'block';
    statusSection.style.display = 'none';

    // Cambiar al tab de presets
    const presetsTab = document.getElementById('presets-tab');
    if (presetsTab) {
        const tab = new bootstrap.Tab(presetsTab);
        tab.show();
    }
}

function showStatus() {
    loginSection.style.display = 'none';
    configSection.style.display = 'none';
    statusSection.style.display = 'block';
}

function renderPresetsList(presets) {
    presetsList.innerHTML = '';
    if (presets.length === 0) {
        presetsList.innerHTML = '<li class="list-group-item text-muted">No hay presets guardados.</li>';
        return;
    }

    presets.forEach(preset => {
        const item = document.createElement('li');
        item.className = 'list-group-item d-flex justify-content-between align-items-center';
        item.innerHTML = `
            <span>${preset}</span>
            <div>
                <button class="btn btn-primary btn-sm execute-preset-btn me-2" data-preset="${preset}">
                    <i class="bi bi-play-fill"></i> Ejecutar
                </button>
                <button class="btn btn-danger btn-sm delete-preset-btn" data-preset="${preset}">
                    <i class="bi bi-trash-fill"></i> Eliminar
                </button>
            </div>
        `;
        presetsList.appendChild(item);
    });

    // Agregar event listeners a los botones
    document.querySelectorAll('.execute-preset-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const presetName = e.target.closest('button').dataset.preset;
            executePreset(presetName);
        });
    });

    document.querySelectorAll('.delete-preset-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const presetName = e.target.closest('button').dataset.preset;
            deletePreset(presetName);
        });
    });
}

async function executePreset(presetName) {
    try {
        const response = await fetch(`/presets/${presetName}`, {
            headers: { 'x-username': currentUser }
        });

        const data = await response.json();
        if (data.config) {
            // Mostrar estado inline
            document.getElementById('execution-status').style.display = 'block';
            document.getElementById('execution-task-status').textContent = 'Estado: Iniciando...';
            document.getElementById('execution-logs').innerHTML = '';

            // Ejecutar directamente
            const config = data.config;
            currentApiKey = config.apiKey;
            const execResponse = await fetch('/api/sync', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': config.apiKey
                },
                body: JSON.stringify({
                    targetServer: config.targetServer,
                    targetDb: config.targetDb,
                    targetUser: config.targetUser,
                    targetPass: config.targetPass,
                    cloneServer: config.cloneServer,
                    cloneDb: config.cloneDb,
                    cloneUser: config.cloneUser,
                    clonePass: config.clonePass,
                    tables: config.tables,
                    sync: config.sync
                })
            });

            const execData = await execResponse.json();
            if (execResponse.ok) {
                taskId = execData.taskId;
                startInlinePolling();
            } else {
                showInlineError(execData.error || 'Error iniciando tarea');
            }
        } else {
            showInlineError('Error cargando preset');
        }
    } catch (error) {
        showInlineError('Error ejecutando preset');
    }
}

function showExecuteAllModal() {
    const modal = new bootstrap.Modal(document.getElementById('executeAllModal'));
    modal.show();
}

async function executeAllPresets() {
    document.getElementById('execution-status').style.display = 'block';
    document.getElementById('execution-task-status').textContent = 'Estado: Ejecutando todos los presets...';
    const logsDiv = document.getElementById('execution-logs');
    logsDiv.innerHTML = '';

    try {
        const response = await fetch('/presets', {
            headers: { 'x-username': currentUser }
        });

        const data = await response.json();
        const presets = data.presets;

        if (presets.length === 0) {
            logsDiv.innerHTML = '<div class="log-entry log-info">No hay presets para ejecutar.</div>';
            document.getElementById('execution-task-status').textContent = 'Estado: Completado';
            return;
        }

        for (const preset of presets) {
            document.getElementById('execution-task-status').textContent = `Estado: Ejecutando ${preset}...`;
            logsDiv.innerHTML += `<div class="log-entry log-info">Iniciando preset: ${preset}</div>`;
            let lastLogLength = 0;
            let lastResultLength = 0;
            await executeAndPollInline(preset, (pollData) => {
                // Append only new logs and results
                if (pollData.logs && pollData.logs.length > lastLogLength) {
                    pollData.logs.slice(lastLogLength).forEach(log => {
                        logsDiv.innerHTML += `<div class="log-entry log-info">${log}</div>`;
                    });
                    lastLogLength = pollData.logs.length;
                }
                if (pollData.results && pollData.results.length > lastResultLength) {
                    pollData.results.slice(lastResultLength).forEach(result => {
                        logsDiv.innerHTML += `<div class="log-entry ${result.status === 'success' ? 'log-success' : 'log-error'}">${result.table}: ${result.message}</div>`;
                    });
                    lastResultLength = pollData.results.length;
                }
                // Auto-scroll to bottom
                logsDiv.scrollTop = logsDiv.scrollHeight;
            });
            logsDiv.innerHTML += `<div class="log-entry log-success">Preset ${preset} completado.</div>`;
        }

        logsDiv.innerHTML += '<div class="log-entry log-success">Todos los presets ejecutados.</div>';
        document.getElementById('execution-task-status').textContent = 'Estado: Completado';
    } catch (error) {
        logsDiv.innerHTML += '<div class="log-entry log-error">Error ejecutando presets.</div>';
        document.getElementById('execution-task-status').textContent = 'Estado: Error';
    }
}

async function executePresetSequentiallyInline(presetName) {
    return new Promise(async (resolve) => {
        try {
            const response = await fetch(`/presets/${presetName}`, {
                headers: { 'x-username': currentUser }
            });
    
            const data = await response.json();
            if (data.config) {
                const config = data.config;
                const execResponse = await fetch('/api/sync', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': config.apiKey
                    },
                    body: JSON.stringify({
                        targetServer: config.targetServer,
                        targetDb: config.targetDb,
                        targetUser: config.targetUser,
                        targetPass: config.targetPass,
                        cloneServer: config.cloneServer,
                        cloneDb: config.cloneDb,
                        cloneUser: config.cloneUser,
                        clonePass: config.clonePass,
                        tables: config.tables,
                        sync: config.sync
                    })
                });

                const execData = await execResponse.json();
                if (execResponse.ok) {
                    const taskIdLocal = execData.taskId;
                    // Polling hasta que termine
                    const poll = async () => {
                        try {
                            const pollResponse = await fetch(`/api/sync/${taskIdLocal}`, {
                                headers: { 'x-api-key': config.apiKey }
                            });
                            const pollData = await pollResponse.json();
                            if (pollData.status === 'completed' || pollData.status === 'failed') {
                                resolve();
                            } else {
                                setTimeout(poll, 2000);
                            }
                        } catch (error) {
                            console.error('Error polling:', error);
                            resolve();
                        }
                    };
                    poll();
                } else {
                    resolve();
                }
            } else {
                resolve();
            }
        } catch (error) {
            console.error('Error ejecutando preset:', error);
            resolve();
        }
    });
}

async function executeAndPollInline(presetName, onStatusUpdate) {
    return new Promise(async (resolve) => {
        try {
            const response = await fetch(`/presets/${presetName}`, {
                headers: { 'x-username': currentUser }
            });

            const data = await response.json();
            if (data.config) {
                const config = data.config;
                const execResponse = await fetch('/api/sync', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': config.apiKey
                    },
                    body: JSON.stringify({
                        targetServer: config.targetServer,
                        targetDb: config.targetDb,
                        targetUser: config.targetUser,
                        targetPass: config.targetPass,
                        cloneServer: config.cloneServer,
                        cloneDb: config.cloneDb,
                        cloneUser: config.cloneUser,
                        clonePass: config.clonePass,
                        tables: config.tables,
                        sync: config.sync
                    })
                });

                const execData = await execResponse.json();
                if (execResponse.ok) {
                    const taskIdLocal = execData.taskId;
                    const poll = async () => {
                        try {
                            const pollResponse = await fetch(`/api/sync/${taskIdLocal}`, {
                                headers: { 'x-api-key': config.apiKey }
                            });
                            const pollData = await pollResponse.json();
                            onStatusUpdate(pollData);
                            if (pollData.status === 'completed' || pollData.status === 'failed') {
                                resolve();
                            } else {
                                setTimeout(poll, 2000);
                            }
                        } catch (error) {
                            console.error('Error polling:', error);
                            resolve();
                        }
                    };
                    poll();
                } else {
                    onStatusUpdate({
                        status: 'failed',
                        logs: [],
                        results: [{ table: 'N/A', status: 'error', message: execData.error || 'Error iniciando tarea' }]
                    });
                    resolve();
                }
            } else {
                onStatusUpdate({
                    status: 'failed',
                    logs: [],
                    results: [{ table: 'N/A', status: 'error', message: 'Error cargando preset' }]
                });
                resolve();
            }
        } catch (error) {
            console.error('Error ejecutando preset:', error);
            onStatusUpdate({
                status: 'failed',
                logs: [],
                results: [{ table: 'N/A', status: 'error', message: 'Error de conexión' }]
            });
            resolve();
        }
    });
}

async function deletePreset(presetName) {
    if (!confirm(`¿Eliminar el preset "${presetName}"?`)) return;

    try {
        const response = await fetch(`/presets/${presetName}`, {
            method: 'DELETE',
            headers: { 'x-username': currentUser }
        });

        if (response.ok) {
            loadPresets(); // Recargar lista
        } else {
            showError('config-error', 'Error eliminando preset');
        }
    } catch (error) {
        showError('config-error', 'Error de conexión');
    }
}

function showError(elementId, message) {
    const element = document.getElementById(elementId);
    element.textContent = message;
    element.style.display = 'block';
    setTimeout(() => {
        element.style.display = 'none';
        element.textContent = '';
    }, 5000);
}

function showScheduleModal() {
    const select = document.getElementById('schedule-preset-select');
    select.innerHTML = '<option value="">Seleccionar preset...</option>';

    // Populate with current presets
    const options = document.querySelectorAll('#preset-select option');
    options.forEach(option => {
        if (option.value) {
            const newOption = document.createElement('option');
            newOption.value = option.value;
            newOption.textContent = option.textContent;
            select.appendChild(newOption);
        }
    });

    // Reset form
    document.getElementById('frequency').value = '';
    updateFrequencyFields();

    // Add event listener
    document.getElementById('frequency').addEventListener('change', updateFrequencyFields);

    const modal = new bootstrap.Modal(document.getElementById('scheduleModal'));
    modal.show();
}

function updateFrequencyFields() {
    const freq = document.getElementById('frequency').value;
    document.getElementById('minutes-group').style.display = freq === 'minutes' ? 'block' : 'none';
    document.getElementById('hours-group').style.display = freq === 'hours' ? 'block' : 'none';
    document.getElementById('daily-group').style.display = freq === 'daily' ? 'block' : 'none';
    document.getElementById('weekly-group').style.display = freq === 'weekly' ? 'block' : 'none';
    document.getElementById('monthly-group').style.display = freq === 'monthly' ? 'block' : 'none';
}

function generateCron() {
    const freq = document.getElementById('frequency').value;
    if (freq === 'minutes') {
        const min = document.getElementById('minutes-input').value;
        if (!min || min < 1 || min > 59) return null;
        return `*/${min} * * * *`;
    } else if (freq === 'hours') {
        const hour = document.getElementById('hours-input').value;
        if (!hour || hour < 1 || hour > 23) return null;
        return `0 */${hour} * * *`;
    } else if (freq === 'daily') {
        const time = document.getElementById('daily-time').value;
        if (!time) return null;
        const [h, m] = time.split(':');
        return `${m} ${h} * * *`;
    } else if (freq === 'weekly') {
        const day = document.getElementById('weekly-day').value;
        const time = document.getElementById('weekly-time').value;
        if (!time) return null;
        const [h, m] = time.split(':');
        return `${m} ${h} * * ${day}`;
    } else if (freq === 'monthly') {
        const day = document.getElementById('monthly-day').value;
        const time = document.getElementById('monthly-time').value;
        if (!day || !time || day < 1 || day > 31) return null;
        const [h, m] = time.split(':');
        return `${m} ${h} ${day} * *`;
    }
    return null;
}

async function schedulePreset() {
    const preset = document.getElementById('schedule-preset-select').value;
    const cronExpression = generateCron();

    if (!preset || !cronExpression) {
        alert('Seleccione un preset y configure la frecuencia válida');
        return;
    }

    try {
        const response = await fetch('/schedule', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': 'default-api-key',
                'x-username': currentUser
            },
            body: JSON.stringify({ preset, cronExpression })
        });

        const data = await response.json();
        if (response.ok) {
            alert('Preset programado exitosamente');
            loadSchedules();
            bootstrap.Modal.getInstance(document.getElementById('scheduleModal')).hide();
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Error de conexión');
    }
}

async function loadSchedules() {
    try {
        const response = await fetch('/schedules', {
            headers: { 'x-username': currentUser, 'x-api-key': 'default-api-key' }
        });

        const data = await response.json();
        renderSchedulesList(data.schedules || []);
    } catch (error) {
        console.error('Error cargando schedules:', error);
    }
}

function renderSchedulesList(schedules) {
    schedulesList.innerHTML = '';

    if (schedules.length === 0) {
        schedulesList.innerHTML = '<li class="list-group-item text-muted">No hay trabajos programados.</li>';
        return;
    }

    schedules.forEach(schedule => {
        const item = document.createElement('li');
        item.className = 'list-group-item d-flex justify-content-between align-items-center';
        item.innerHTML = `
            <span>${schedule.preset}</span>
            <button class="btn btn-danger btn-sm stop-schedule-btn" data-id="${schedule.id}">
                <i class="bi bi-stop-fill"></i> Detener
            </button>
        `;
        schedulesList.appendChild(item);
    });

    // Add event listeners
    document.querySelectorAll('.stop-schedule-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const id = e.target.closest('button').dataset.id;
            stopSchedule(id);
        });
    });
}

async function stopSchedule(id) {
    if (!confirm('¿Detener este trabajo programado?')) return;

    try {
        const response = await fetch(`/schedule/${id}`, {
            method: 'DELETE',
            headers: { 'x-username': currentUser, 'x-api-key': 'default-api-key' }
        });

        if (response.ok) {
            loadSchedules();
        } else {
            alert('Error deteniendo trabajo');
        }
    } catch (error) {
        alert('Error de conexión');
    }
}