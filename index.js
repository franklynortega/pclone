import 'dotenv/config';
import fs from 'fs';

// Parse command line arguments
let targetDb = null;
let cloneDb = null;
let sync = false;
const filteredArgs = [];

const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--target-db' && i + 1 < args.length) {
    targetDb = args[i + 1];
    i++; // skip value
  } else if (args[i] === '--clone-db' && i + 1 < args.length) {
    cloneDb = args[i + 1];
    i++; // skip value
  } else if (args[i] === '--sync') {
    sync = true;
  } else {
    filteredArgs.push(args[i]);
  }
}

// Override environment variables if provided
if (targetDb) {
  process.env.TARGET_DB = targetDb;
}
if (cloneDb) {
  process.env.CLONE_DB = cloneDb;
}

import { compareTable } from './compare.js';

async function main() {

  let tables = [];

  if (filteredArgs.length === 0) {
    // Procesar todas las tablas desde tables.json
    const tablesData = JSON.parse(fs.readFileSync('tables.json', 'utf8'));
    tables = tablesData;
  } else if (filteredArgs.length === 1) {
    // Una tabla específica, buscar pk en tables.json
    const tableName = filteredArgs[0];
    const tablesData = JSON.parse(fs.readFileSync('tables.json', 'utf8'));
    const tableConfig = tablesData.find(t => t.table === tableName);
    if (tableConfig) {
      tables = [{ table: tableName, pk: tableConfig.pk }];
    } else {
      // Si no está en tables.json, usar pk por defecto
      const pk = process.env.PK_COLUMNS ? JSON.parse(process.env.PK_COLUMNS) : ['id'];
      tables = [{ table: tableName, pk }];
    }
  } else {
    console.log('Uso: node index.js [nombreTabla] [--target-db <db>] [--clone-db <db>] [--sync]');
    console.log('  --target-db: Base de datos target (opcional, usa env TARGET_DB)');
    console.log('  --clone-db: Base de datos clone (opcional, usa env CLONE_DB o target si no especificado)');
    console.log('  --sync: Sincronizar tablas si hay diferencias');
    console.log('Si no se especifica tabla, procesa todas desde tables.json');
    process.exit(1);
  }

  for (const { table, pk } of tables) {
    console.log(`Procesando tabla: ${table} con PK: ${pk.join(', ')}`);
    await compareTable(table, pk, sync);
  }
}

main().catch(console.error);