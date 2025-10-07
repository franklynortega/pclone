import 'dotenv/config';
import { compareTable } from './compare.js';
import fs from 'fs';

async function main() {
  const args = process.argv.slice(2);
  const sync = args.includes('--sync');
  const filteredArgs = args.filter(arg => arg !== '--sync');

  let tables = [];

  if (filteredArgs.length === 0) {
    // Procesar todas las tablas desde tables.json
    const tablesData = JSON.parse(fs.readFileSync('tables.json', 'utf8'));
    tables = tablesData;
  } else if (filteredArgs.length === 1) {
    // Una tabla específica, asumir pk=['id'] si no especificado
    const tableName = filteredArgs[0];
    const pk = process.env.PK_COLUMNS ? JSON.parse(process.env.PK_COLUMNS) : ['id'];
    tables = [{ table: tableName, pk }];
  } else {
    console.log('Uso: node index.js [nombreTabla] [--sync]');
    console.log('Si no se especifica tabla, procesa todas desde tables.json');
    process.exit(1);
  }

  for (const { table, pk } of tables) {
    console.log(`Procesando tabla: ${table} con PK: ${pk.join(', ')}`);
    await compareTable(table, pk, sync);
  }
}

main().catch(console.error);