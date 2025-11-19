import 'dotenv/config';
import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { compareTable } from './compare.js';
import logger from './logger.js';
import fs from 'fs';
import crypto from 'crypto';

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || 'default-api-key';

// Middleware para parsear JSON
app.use(express.json());

// Middleware para servir archivos estáticos de la interfaz web
app.use(express.static('public'));

// Almacén de tareas en memoria (en producción usar Redis o DB)
const tasks = new Map();

// Middleware de autenticación
const authenticate = (req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  if (!apiKey || apiKey !== API_KEY) {
    return res.status(401).json({ error: 'API key inválida' });
  }
  next();
};

// Función para validar payload
const validatePayload = (payload) => {
  const errors = [];

  if (!payload.targetServer) errors.push('targetServer es requerido');
  if (!payload.targetDb) errors.push('targetDb es requerido');
  if (!payload.targetUser) errors.push('targetUser es requerido');
  if (!payload.targetPass) errors.push('targetPass es requerido');
  if (!payload.cloneServer) errors.push('cloneServer es requerido');
  if (!payload.cloneDb) errors.push('cloneDb es requerido');
  if (!payload.cloneUser) errors.push('cloneUser es requerido');
  if (!payload.clonePass) errors.push('clonePass es requerido');
  if (!payload.tables || !Array.isArray(payload.tables)) {
    errors.push('tables debe ser un array');
  } else {
    payload.tables.forEach((table, index) => {
      if (!table.table) errors.push(`tables[${index}].table es requerido`);
      if (!table.pk || !Array.isArray(table.pk)) errors.push(`tables[${index}].pk debe ser un array`);
    });
  }
  if (typeof payload.sync !== 'boolean') errors.push('sync debe ser boolean');

  return errors;
};

// Funciones auxiliares para autenticación y presets
const hashPassword = (password) => crypto.createHash('sha256').update(password).digest('hex');

const loadUsers = () => {
  try {
    const data = fs.readFileSync('users.txt', 'utf8');
    const users = {};
    data.split('\n').forEach(line => {
      const [username, hash] = line.trim().split(':');
      if (username && hash) users[username] = hash;
    });
    return users;
  } catch (error) {
    logger.error('Error cargando users.txt:', error);
    return {};
  }
};

const loadPresets = () => {
  try {
    return JSON.parse(fs.readFileSync('presets.json', 'utf8'));
  } catch (error) {
    logger.error('Error cargando presets.json:', error);
    return {};
  }
};

const savePresets = (presets) => {
  try {
    fs.writeFileSync('presets.json', JSON.stringify(presets, null, 2));
  } catch (error) {
    logger.error('Error guardando presets.json:', error);
  }
};

// Endpoint POST /api/sync
app.post('/api/sync', authenticate, async (req, res) => {
  try {
    const payload = req.body;
    const validationErrors = validatePayload(payload);

    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Payload inválido',
        details: validationErrors
      });
    }

    const taskId = uuidv4();

    // Crear tarea
    tasks.set(taskId, {
      id: taskId,
      status: 'pending',
      createdAt: new Date().toISOString(),
      payload,
      results: [],
      logs: []
    });

    // Procesar de forma asíncrona
    processSyncTask(taskId);

    res.status(202).json({
      taskId,
      status: 'accepted',
      message: 'Sincronización iniciada'
    });

  } catch (error) {
    logger.error('Error en POST /api/sync:', error);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// Endpoint GET /api/sync/:id
app.get('/api/sync/:id', authenticate, (req, res) => {
  const taskId = req.params.id;
  const task = tasks.get(taskId);

  if (!task) {
    return res.status(404).json({ error: 'Tarea no encontrada' });
  }

  res.json({
    id: task.id,
    status: task.status,
    createdAt: task.createdAt,
    completedAt: task.completedAt,
    results: task.results,
    logs: task.logs
  });
});

// Endpoint POST /auth/login
app.post('/auth/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'Usuario y contraseña requeridos' });
  }

  const users = loadUsers();
  const hashedPassword = hashPassword(password);

  if (users[username] === hashedPassword) {
    res.json({ success: true, username });
  } else {
    res.status(401).json({ error: 'Credenciales inválidas' });
  }
});

// Endpoint GET /presets
app.get('/presets', (req, res) => {
  const username = req.headers['x-user'];
  if (!username) {
    return res.status(401).json({ error: 'Usuario no autenticado' });
  }

  const presets = loadPresets();
  const userPresets = presets[username] || {};
  res.json({ presets: Object.keys(userPresets) });
});

// Endpoint POST /presets
app.post('/presets', (req, res) => {
  const username = req.headers['x-user'];
  if (!username) {
    return res.status(401).json({ error: 'Usuario no autenticado' });
  }

  const { name, config } = req.body;
  if (!name || !config) {
    return res.status(400).json({ error: 'Nombre y configuración requeridos' });
  }

  const presets = loadPresets();
  if (!presets[username]) presets[username] = {};
  presets[username][name] = config;
  savePresets(presets);

  res.json({ success: true });
});

// Endpoint GET /presets/:name
app.get('/presets/:name', (req, res) => {
  const username = req.headers['x-user'];
  if (!username) {
    return res.status(401).json({ error: 'Usuario no autenticado' });
  }

  const name = req.params.name;
  const presets = loadPresets();
  const userPresets = presets[username] || {};
  const config = userPresets[name];

  if (!config) {
    return res.status(404).json({ error: 'Preset no encontrado' });
  }

  res.json({ config });
});

// Endpoint GET /tables - devuelve las tablas por defecto
app.get('/tables', (req, res) => {
  try {
    const tables = JSON.parse(fs.readFileSync('tables.json', 'utf8'));
    res.json({ tables });
  } catch (error) {
    logger.error('Error cargando tables.json:', error);
    res.status(500).json({ error: 'Error cargando tablas por defecto' });
  }
});

// Función para procesar la tarea de sync
async function processSyncTask(taskId) {
  const task = tasks.get(taskId);
  if (!task) return;

  task.status = 'processing';
  logger.info(`Iniciando procesamiento de tarea ${taskId}`);

  try {
    // Configurar variables de entorno temporales
    const originalEnv = {
      TARGET_SERVER: process.env.TARGET_SERVER,
      TARGET_DB: process.env.TARGET_DB,
      TARGET_USER: process.env.TARGET_USER,
      TARGET_PASS: process.env.TARGET_PASS,
      CLONE_SERVER: process.env.CLONE_SERVER,
      CLONE_DB: process.env.CLONE_DB,
      CLONE_USER: process.env.CLONE_USER,
      CLONE_PASS: process.env.CLONE_PASS
    };

    process.env.TARGET_SERVER = task.payload.targetServer;
    process.env.TARGET_DB = task.payload.targetDb;
    process.env.TARGET_USER = task.payload.targetUser;
    process.env.TARGET_PASS = task.payload.targetPass;
    process.env.CLONE_SERVER = task.payload.cloneServer;
    process.env.CLONE_DB = task.payload.cloneDb;
    process.env.CLONE_USER = task.payload.cloneUser;
    process.env.CLONE_PASS = task.payload.clonePass;

    for (const tableConfig of task.payload.tables) {
      try {
        logger.info(`Procesando tabla ${tableConfig.table} en tarea ${taskId}`);
        task.logs.push(`Procesando tabla: ${tableConfig.table}`);

        const synced = await compareTable(tableConfig.table, tableConfig.pk, task.payload.sync);

        task.results.push({
          table: tableConfig.table,
          status: 'success',
          message: task.payload.sync ? 'Sincronización completada' : 'Comparación completada',
          changed: !synced
        });

        task.logs.push(`Tabla ${tableConfig.table} procesada exitosamente`);

      } catch (error) {
        logger.error(`Error en tabla ${tableConfig.table} de tarea ${taskId}:`, error);
        task.results.push({
          table: tableConfig.table,
          status: 'error',
          message: error.message,
          changed: null
        });
        task.logs.push(`Error en tabla ${tableConfig.table}: ${error.message}`);
      }
    }

    // Restaurar variables de entorno
    process.env.TARGET_SERVER = originalEnv.TARGET_SERVER;
    process.env.TARGET_DB = originalEnv.TARGET_DB;
    process.env.TARGET_USER = originalEnv.TARGET_USER;
    process.env.TARGET_PASS = originalEnv.TARGET_PASS;
    process.env.CLONE_SERVER = originalEnv.CLONE_SERVER;
    process.env.CLONE_DB = originalEnv.CLONE_DB;
    process.env.CLONE_USER = originalEnv.CLONE_USER;
    process.env.CLONE_PASS = originalEnv.CLONE_PASS;

    task.status = 'completed';
    task.completedAt = new Date().toISOString();
    logger.info(`Tarea ${taskId} completada`);

  } catch (error) {
    task.status = 'failed';
    task.completedAt = new Date().toISOString();
    task.logs.push(`Error general: ${error.message}`);
    logger.error(`Error general en tarea ${taskId}:`, error);
  }
}

// Iniciar servidor
app.listen(PORT, () => {
  logger.info(`Servidor corriendo en puerto ${PORT}`);
});

export default app;