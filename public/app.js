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

// Inicialización
document.addEventListener('DOMContentLoaded', () => {
    loginForm.addEventListener('submit', handleLogin);
    loadPresetBtn.addEventListener('click', loadSelectedPreset);
    savePresetBtn.addEventListener('click', savePreset);
    executeBtn.addEventListener('click', executeTask);
    backBtn.addEventListener('click', showConfig);
    addTableBtn.addEventListener('click', addTableEntry);
    loadDefaultTablesBtn.addEventListener('click', loadDefaultTables);

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
            headers: { 'x-user': currentUser }
        });

        const data = await response.json();
        presetSelect.innerHTML = '<option value="">Seleccionar preset...</option>';
        data.presets.forEach(preset => {
            const option = document.createElement('option');
            option.value = preset;
            option.textContent = preset;
            presetSelect.appendChild(option);
        });
    } catch (error) {
        console.error('Error cargando presets:', error);
    }
}

async function loadSelectedPreset() {
    const presetName = presetSelect.value;
    if (!presetName) return;

    try {
        const response = await fetch(`/presets/${presetName}`, {
            headers: { 'x-user': currentUser }
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
                'x-user': currentUser
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
    entry.className = 'table-entry';
    entry.innerHTML = `
        <input type="text" class="table-name" placeholder="Nombre de tabla" required>
        <input type="text" class="table-pk" placeholder="Claves primarias (separadas por coma)" required>
        <button class="remove-table-btn">Remover</button>
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

// Funciones de UI
function showConfig() {
    loginSection.style.display = 'none';
    configSection.style.display = 'block';
    statusSection.style.display = 'none';
}

function showStatus() {
    loginSection.style.display = 'none';
    configSection.style.display = 'none';
    statusSection.style.display = 'block';
}

function showError(elementId, message) {
    const element = document.getElementById(elementId);
    element.textContent = message;
    setTimeout(() => element.textContent = '', 5000);
}