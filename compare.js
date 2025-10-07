import sql from 'mssql';
import { targetConfig, cloneConfig } from './config.js';
import logger from './logger.js';
import retry from 'async-retry';

async function getChecksum(pool, tableName) {
  return await retry(async (bail) => {
    try {
      const query = `SELECT CHECKSUM_AGG(CHECKSUM(*)) AS checksum FROM ${tableName}`;
      const result = await pool.request().query(query);
      return result.recordset[0].checksum;
    } catch (error) {
      logger.warn(`Error obteniendo checksum para ${tableName}, reintentando...`, error.message);
      throw error; // retry
    }
  }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
}

async function getColumns(pool, tableName) {
  return await retry(async () => {
    const query = `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${tableName}' ORDER BY ORDINAL_POSITION`;
    const result = await pool.request().query(query);
    return result.recordset.map(row => ({ name: row.COLUMN_NAME, type: row.DATA_TYPE }));
  }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
}

function formatValue(val, type) {
  if (val === null) return 'NULL';
  if (type && (type.toLowerCase().includes('binary') || type.toLowerCase().includes('image'))) {
    // Para VARBINARY, IMAGE, etc., convertir buffer a hex
    if (Buffer.isBuffer(val)) {
      return `0x${val.toString('hex')}`;
    }
    return 'NULL'; // Si no es buffer, asumir null
  }
  if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`;
  return val;
}

async function syncTable(tableName, pkColumns) {
  let targetPool, clonePool;
  try {
    targetPool = await sql.connect(targetConfig);
    clonePool = await sql.connect(cloneConfig);

    const columns = await getColumns(targetPool, tableName);
    if (columns.length === 0) throw new Error('Tabla no encontrada en target');

    const columnNames = columns.map(col => col.name);
    for (const pk of pkColumns) {
      if (!columnNames.includes(pk)) throw new Error(`Columna ${pk} no encontrada`);
    }

    // Leer datos de target
    const selectQuery = `SELECT * FROM ${tableName}`;
    const targetResult = await retry(async () => await targetPool.request().query(selectQuery), { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
    const rows = targetResult.recordset;

    if (rows.length === 0) {
      console.log('No hay datos para sincronizar.');
      return;
    }

    // Procesar en batches
    const batchSize = 100;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);

      // Construir VALUES para MERGE
      const values = batch.map(row => `(${columnNames.map(colName => {
        const col = columns.find(c => c.name === colName);
        const val = row[colName];
        return formatValue(val, col.type);
      }).join(', ')})`).join(', ');

      const setClause = columns.filter(col => !pkColumns.includes(col)).map(col => `${col} = source.${col}`).join(', ');
      const insertCols = columns.join(', ');
      const insertVals = columns.map(col => `source.${col}`).join(', ');

      const onClause = pkColumns.map(pk => `targetTable.${pk} = source.${pk}`).join(' AND ');

      const mergeQuery = `
        MERGE ${tableName} AS targetTable
        USING (VALUES ${values}) AS source (${columnNames.join(', ')})
        ON ${onClause}
        WHEN MATCHED THEN
          UPDATE SET ${setClause}
        WHEN NOT MATCHED THEN
          INSERT (${insertCols}) VALUES (${insertVals});
      `;

      await retry(async () => {
        await clonePool.request().query(mergeQuery);
      }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
    }

    logger.info(`Sincronización completada para ${tableName}`);
  } catch (error) {
    logger.error('Error en sincronización:', error);
  } finally {
    if (targetPool) await targetPool.close();
    if (clonePool) await clonePool.close();
  }
}

async function compareTable(tableName, pkColumns, sync = false) {
  let targetPool, clonePool;
  try {
    targetPool = await sql.connect(targetConfig);
    clonePool = await sql.connect(cloneConfig);

    const targetChecksum = await getChecksum(targetPool, tableName);
    const cloneChecksum = await getChecksum(clonePool, tableName);

    if (targetChecksum === cloneChecksum) {
      logger.info(`Tabla ${tableName} está sincronizada.`);
      return true;
    } else {
      logger.warn(`Tabla ${tableName} no está sincronizada.`);
      if (sync) {
        await syncTable(tableName, pkColumns);
      }
      return false;
    }
  } catch (error) {
    logger.error('Error en comparación:', error);
    return false;
  } finally {
    if (targetPool) await targetPool.close();
    if (clonePool) await clonePool.close();
  }
}

export { compareTable };