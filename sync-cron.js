import 'dotenv/config';
import { compareTable } from './compare.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import logger from './logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const lockFile = path.join(__dirname, 'sync.lock');

async function runSync() {
  // Verificar lock
  if (fs.existsSync(lockFile)) {
    logger.warn('Otra instancia de sync-cron.js está ejecutándose. Saltando.');
    return;
  }

  // Crear lock
  fs.writeFileSync(lockFile, process.pid.toString());

  try {
    logger.info('Iniciando sincronización automática:', { timestamp: new Date().toISOString() });

    const tablesData = JSON.parse(fs.readFileSync('tables.json', 'utf8')).sort((a, b) => a.priority - b.priority);
    let lastSync = {};
    if (fs.existsSync('lastSync.json')) {
      lastSync = JSON.parse(fs.readFileSync('lastSync.json', 'utf8'));
    }

    const now = Date.now();

    for (const { table, pk, syncIntervalMinutes, priority } of tablesData) {
      const last = lastSync[table] || 0;
      const intervalMs = syncIntervalMinutes * 60 * 1000;
      if (now - last >= intervalMs) {
        logger.info(`Procesando tabla: ${table}`, { pk: pk.join(', '), syncIntervalMinutes });
        try {
          await compareTable(table, pk, true); // sync=true
          lastSync[table] = now;
        } catch (error) {
          logger.error(`Error en tabla ${table}:`, error);
        }
      } else {
        logger.debug(`Saltando tabla ${table}, próxima sync en ${Math.ceil((intervalMs - (now - last)) / 60000)} min`);
      }
    }

    fs.writeFileSync('lastSync.json', JSON.stringify(lastSync, null, 2));
    logger.info('Sincronización completada.');
  } finally {
    // Remover lock
    if (fs.existsSync(lockFile)) {
      fs.unlinkSync(lockFile);
    }
  }
}

runSync().catch(error => {
  logger.error('Error fatal en sync-cron.js:', error);
  // Remover lock en caso de error
  if (fs.existsSync(lockFile)) {
    fs.unlinkSync(lockFile);
  }
});