import sql from 'mssql';
import { getConfig } from './config.js';
import logger from './logger.js';
import retry from 'async-retry';

async function getChecksum(pool, tableName, pkColumns) {
  return await retry(async (bail) => {
    try {
      // Obtener columnas insertables (excluyendo timestamp, xml, etc.)
      const columns = await getColumns(pool, tableName);
      const insertableColumns = columns.filter(col => !col.type.toLowerCase().includes('timestamp') && !col.isComputed);
      const updatableColumns = insertableColumns.filter(col => !pkColumns.includes(col.name));
      const checksumColumns = updatableColumns.filter(col => !col.type.toLowerCase().includes('xml') && !col.type.toLowerCase().includes('uniqueidentifier') && !col.type.toLowerCase().includes('datetime') && !col.type.toLowerCase().includes('smalldatetime')).map((col, i) => {
        const colName = `[${col.name}]`;
        let expr;
        if (col.type.toLowerCase().includes('char') || col.type.toLowerCase().includes('varchar') || col.type.toLowerCase().includes('nvarchar')) {
          const length = (col.maxLength && col.maxLength > 0) ? col.maxLength : 1000;
          expr = `LEFT(ISNULL(${colName}, ''), ${length})`;
        } else if (col.type.toLowerCase().includes('int') || col.type.toLowerCase().includes('decimal') || col.type.toLowerCase().includes('float') || col.type.toLowerCase().includes('bit')) {
          expr = `ISNULL(${colName}, 0)`;
        } else if (col.type.toLowerCase().includes('datetime') || col.type.toLowerCase().includes('smalldatetime')) {
          expr = `ISNULL(${colName}, '1900-01-01')`;
        } else {
          expr = colName;
        }
        return `${expr} AS col${i + 1}`;
      });

      if (checksumColumns.length === 0) {
        // Si no hay columnas válidas, usar COUNT
        const countQuery = `SELECT COUNT(*) AS checksum FROM ${tableName}`;
        const countResult = await pool.request().query(countQuery);
        return countResult.recordset[0].checksum;
      }

      const orderBy = pkColumns.map(pk => `[${pk}]`).join(', ');
      const query = `WITH ordered AS (SELECT TOP 100 PERCENT ${checksumColumns.join(', ')} FROM ${tableName} ORDER BY ${orderBy}) SELECT CHECKSUM_AGG(CHECKSUM(${checksumColumns.map((col, i) => `col${i + 1}`).join(', ')})) AS checksum FROM ordered`;
      logger.debug(`Query checksum para ${tableName}: ${query}`);
      const result = await pool.request().query(query);
      return result.recordset[0].checksum;
    } catch (error) {
      logger.warn(`Error obteniendo checksum para ${tableName}, reintentando...`, error.message);
      throw error; // retry
    }
  }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
}

async function getChecksumWithColumns(pool, tableName, pkColumns, columns, minLengths) {
  return await retry(async (bail) => {
    try {
      const insertableColumns = columns.filter(col => !col.type.toLowerCase().includes('timestamp') && !col.isComputed);
      let allColumns = insertableColumns.filter(col => !pkColumns.includes(col.name) && (col.type.toLowerCase().includes('char') || col.type.toLowerCase().includes('int') || col.type.toLowerCase().includes('decimal') || col.type.toLowerCase().includes('float') || col.type.toLowerCase().includes('bit') || col.type.toLowerCase().includes('money') || col.type.toLowerCase().includes('numeric') || col.type.toLowerCase().includes('smallint') || col.type.toLowerCase().includes('tinyint') || col.type.toLowerCase().includes('bigint') || col.type.toLowerCase().includes('real')) && !col.type.toLowerCase().includes('xml') && col.name.toLowerCase() !== 'validador' && col.name.toLowerCase() !== 'co_alma_calculado' && col.name.toLowerCase() !== 'fe_us_in' && col.name.toLowerCase() !== 'fe_us_mo' && col.name.toLowerCase() !== 'fecha_reg' && !col.type.toLowerCase().includes('uniqueidentifier') && !col.type.toLowerCase().includes('binary') && !col.type.toLowerCase().includes('image') && !col.type.toLowerCase().includes('text') && !col.type.toLowerCase().includes('datetime') && !col.type.toLowerCase().includes('smalldatetime'));

      // Excluir rowguid ya que es único por fila y causa checksums diferentes
      allColumns = allColumns.filter(col => col.name !== 'rowguid');

      const checksumColumns = allColumns.filter(col => !col.type.toLowerCase().includes('xml') && col.name.toLowerCase() !== 'validador' && col.name.toLowerCase() !== 'fe_us_in' && col.name.toLowerCase() !== 'fe_us_mo' && col.name.toLowerCase() !== 'co_us_in' && col.name.toLowerCase() !== 'co_sucu_in' && col.name.toLowerCase() !== 'co_us_mo' && col.name.toLowerCase() !== 'co_sucu_mo' && col.name !== 'co_mone' && col.name !== 'revisado' && col.name !== 'trasnfe' && !col.type.toLowerCase().includes('uniqueidentifier') && !col.type.toLowerCase().includes('binary') && !col.type.toLowerCase().includes('datetime') && !col.type.toLowerCase().includes('smalldatetime') && !col.type.toLowerCase().includes('bit')).map((col, i) => {
        const colName = `[${col.name}]`;
        let expr;
        if (col.type.toLowerCase().includes('char') || col.type.toLowerCase().includes('varchar') || col.type.toLowerCase().includes('nvarchar')) {
          const length = 4000; // Use fixed large length to ignore column length differences
          expr = `LEFT(ISNULL(${colName}, ''), ${length})`;
        } else if (col.type.toLowerCase().includes('int') || col.type.toLowerCase().includes('decimal') || col.type.toLowerCase().includes('float') || col.type.toLowerCase().includes('bit') || col.type.toLowerCase().includes('money') || col.type.toLowerCase().includes('numeric') || col.type.toLowerCase().includes('smallint') || col.type.toLowerCase().includes('tinyint') || col.type.toLowerCase().includes('bigint') || col.type.toLowerCase().includes('real')) {
          expr = `CAST(ISNULL(${colName}, 0) AS decimal(18,5))`;
        } else if (col.type.toLowerCase().includes('datetime') || col.type.toLowerCase().includes('smalldatetime')) {
          if (tableName === 'saArtPrecio' || tableName === 'saArticulo') {
            // Formato 120 es 'yyyy-mm-dd hh:mi:ss' (24h)
            expr = `CONVERT(nvarchar(19), ISNULL(${colName}, '1900-01-01'), 120)`;
          } else {
            expr = `ISNULL(${colName}, '1900-01-01')`;
          }
        } else {
          expr = colName;
        }
        return `${expr} AS col${i + 1}`;
      });

      if (checksumColumns.length === 0) {
        // Si no hay columnas válidas, usar COUNT
        const countQuery = `SELECT COUNT(*) AS checksum FROM ${tableName}`;
        const countResult = await pool.request().query(countQuery);
        return countResult.recordset[0].checksum;
      }

      const orderBy = pkColumns.map(pk => `[${pk}]`).join(', ');
      const query = `WITH ordered AS (SELECT TOP 100 PERCENT ${checksumColumns.join(', ')} FROM ${tableName} ORDER BY ${orderBy}) SELECT CHECKSUM_AGG(CHECKSUM(${checksumColumns.map((col, i) => `col${i + 1}`).join(', ')})) AS checksum FROM ordered`;
      logger.debug(`Query checksum para ${tableName}: ${query}`);
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
    const query = `
      SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE, sc.is_computed
      FROM INFORMATION_SCHEMA.COLUMNS c
      INNER JOIN sys.tables t ON c.TABLE_NAME = t.name
      INNER JOIN sys.columns sc ON sc.name = c.COLUMN_NAME AND sc.object_id = t.object_id
      WHERE t.name = '${tableName}' AND c.TABLE_NAME = '${tableName}'
      ORDER BY c.ORDINAL_POSITION
    `;
    const result = await pool.request().query(query);
    return result.recordset.map(row => ({
      name: row.COLUMN_NAME,
      type: row.DATA_TYPE,
      maxLength: row.CHARACTER_MAXIMUM_LENGTH,
      precision: row.NUMERIC_PRECISION,
      scale: row.NUMERIC_SCALE,
      nullable: row.IS_NULLABLE === 'YES',
      isComputed: row.is_computed === 1
    }));
  }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
}

function formatValue(val, type, maxLength) {
  if (val === null) {
    if (type && type.toLowerCase().includes('bit')) {
      return '0'; // Para bit, usar 0 en lugar de NULL
    }
    return 'NULL';
  }
  if (type && (type.toLowerCase().includes('binary') || type.toLowerCase().includes('image'))) {
    // Para VARBINARY, IMAGE, etc., convertir buffer a hex
    if (Buffer.isBuffer(val)) {
      return `0x${val.toString('hex')}`;
    }
    return 'NULL'; // Si no es buffer, asumir null
  }
  if (typeof val === 'string') {
    return `'${val.replace(/'/g, "''")}'`; // escape quotes, no truncate
  }
  if (val instanceof Date) return `'${val.toISOString().slice(0, 19).replace('T', ' ')}'`; // formato YYYY-MM-DD HH:MM:SS
  if (typeof val === 'boolean') return val ? '1' : '0'; // bit
  return val;
}

function formatValueForParam(val, type) {
  if (val === null) return sql.NText;
  if (type && type.toLowerCase().includes('int')) return sql.Int;
  if (type && type.toLowerCase().includes('decimal')) return sql.Decimal(18, 2);
  if (type && type.toLowerCase().includes('varchar')) return sql.NText;
  if (type && type.toLowerCase().includes('nvarchar')) return sql.NText;
  if (type && type.toLowerCase().includes('datetime')) return sql.DateTime;
  if (type && type.toLowerCase().includes('bit')) return sql.Bit;
  if (type && (type.toLowerCase().includes('binary') || type.toLowerCase().includes('image'))) return sql.VarBinary(sql.MAX);
  // Default
  return sql.NText;
}

async function syncTable(tableName, pkColumns) {
  let targetPool, clonePool;
  try {
    const { targetConfig, cloneConfig } = getConfig();
    targetPool = await new sql.ConnectionPool(targetConfig).connect();
    clonePool = await new sql.ConnectionPool(cloneConfig).connect();

    const columns = await getColumns(clonePool, tableName);
    const targetColumns = await getColumns(targetPool, tableName);
    if (columns.length === 0) throw new Error('Tabla no encontrada en target');

    const columnNames = columns.map(col => col.name);
    for (const pk of pkColumns) {
      if (!columnNames.includes(pk)) throw new Error(`Columna ${pk} no encontrada`);
    }

    // Filtrar pkColumns para excluir columnas computadas
    pkColumns = pkColumns.filter(pk => {
      const col = columns.find(c => c.name === pk);
      return col && !col.isComputed;
    });
    // Excluir columnas computadas conocidas que no se detectaron
    if (tableName === 'saArtPrecio') {
      pkColumns = pkColumns.filter(pk => pk !== 'co_alma_calculado');
    }

    // Filtrar columnas que no se pueden insertar (timestamp, computed, etc.)
    let insertableColumns = columns.filter(col => !col.type.toLowerCase().includes('timestamp') && !col.isComputed && col.name !== 'rowguid');
    // Excluir columnas computadas conocidas que no se detectaron
    if (tableName === 'saArtPrecio') {
      insertableColumns = insertableColumns.filter(col => col.name !== 'co_alma_calculado');
    }
    const insertableColumnNames = insertableColumns.map(col => col.name);

    // Leer datos de target
    const selectQuery = `SELECT ${insertableColumnNames.join(', ')} FROM ${tableName}`;
    const targetResult = await retry(async () => await targetPool.request().query(selectQuery), { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
    const rows = targetResult.recordset;

    if (rows.length === 0) {
      return;
    }

    // Crear tabla temporal global en clone con datos de target
    const tempTableName = `##temp_${tableName}_${Date.now()}`;
    const createTempQuery = `CREATE TABLE ${tempTableName} (${insertableColumns.map(col => {
      let type = col.type;
      if (col.type === 'char' || col.type === 'varchar' || col.type === 'nvarchar') {
        type = 'nvarchar(max)';
      } else if (col.type === 'decimal' || col.type === 'numeric') {
        if (col.precision && col.scale !== undefined) {
          type += `(${col.precision}, ${col.scale})`;
        } else {
          type += '(18, 2)'; // default
        }
      } else {
        if (col.maxLength && col.maxLength > 0) {
          type += `(${col.maxLength})`;
        }
      }
      return `[${col.name}] ${type}`;
    }).join(', ')})`;
    try {
      await clonePool.request().query(createTempQuery);
    } catch (error) {
      throw error;
    }

    // Insertar datos en temp table fila por fila
    for (const row of rows) {
      const values = insertableColumnNames.map(colName => {
        const col = insertableColumns.find(c => c.name === colName);
        const targetCol = targetColumns.find(tc => tc.name === colName);
        const targetLen = (targetCol && targetCol.maxLength && targetCol.maxLength > 0) ? targetCol.maxLength : 1000;
        const cloneLen = (col.maxLength && col.maxLength > 0) ? col.maxLength : 1000;
        const tempLen = Math.max(targetLen, cloneLen);
        const val = row[colName];
        return formatValue(val, col.type, tempLen);
      }).join(', ');

      const insertTempQuery = `INSERT INTO ${tempTableName} (${insertableColumnNames.join(', ')}) VALUES (${values})`;

      await retry(async () => {
        await clonePool.request().query(insertTempQuery);
      }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
    }

    // Para saArtPrecio, usar DELETE/INSERT directo sin temp table
    if (tableName === 'saArtPrecio') {
      // DELETE all from clone
      await clonePool.request().query(`DELETE FROM ${tableName}`);

      // INSERT rows one by one
      for (const row of rows) {
        const values = insertableColumnNames.map(colName => {
          const col = insertableColumns.find(c => c.name === colName);
          const targetCol = targetColumns.find(tc => tc.name === colName);
          const targetLen = (targetCol && targetCol.maxLength && targetCol.maxLength > 0) ? targetCol.maxLength : 1000;
          const cloneLen = (col.maxLength && col.maxLength > 0) ? col.maxLength : 1000;
          const tempLen = Math.max(targetLen, cloneLen);
          const val = row[colName];
          return formatValue(val, col.type, tempLen);
        }).join(', ');

        const insertQuery = `INSERT INTO ${tableName} (${insertableColumnNames.join(', ')}) VALUES (${values})`;

        await retry(async () => {
          await clonePool.request().query(insertQuery);
        }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
      }

      logger.info(`DELETE/INSERT afectó ${rows.length} filas`);
    } else {
      // MERGE from temp to handle updates and inserts without deleting
      const mergeCols = insertableColumns.map(col => `[${col.name}]`).join(', ');
      const mergeVals = insertableColumns.map(col => `source.[${col.name}]`).join(', ');
      const pkCondition = pkColumns.map(pk => `target.[${pk}] = source.[${pk}]`).join(' AND ');
      const updateSet = insertableColumns.filter(col => !pkColumns.includes(col.name)).map(col => `target.[${col.name}] = source.[${col.name}]`).join(', ');

      const mergeQuery = `
        MERGE ${tableName} AS target
        USING ${tempTableName} AS source
        ON ${pkCondition}
        WHEN MATCHED THEN
          UPDATE SET ${updateSet}
        WHEN NOT MATCHED THEN
          INSERT (${mergeCols})
          VALUES (${mergeVals})
        WHEN NOT MATCHED BY SOURCE THEN
          DELETE;
      `;

      await retry(async () => {
        const result = await clonePool.request().query(mergeQuery);
        logger.info(`MERGE afectó ${result.rowsAffected} filas`);
      }, { retries: 3, minTimeout: 1000, maxTimeout: 5000 });
    }

    // Drop temp table
    await clonePool.request().query(`DROP TABLE ${tempTableName}`);

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
    const { targetConfig, cloneConfig } = getConfig();
    targetPool = await new sql.ConnectionPool(targetConfig).connect();
    clonePool = await new sql.ConnectionPool(cloneConfig).connect();

    // Get columns from both to find common non-computed columns
    const targetColumns = await getColumns(targetPool, tableName);
    const cloneColumns = await getColumns(clonePool, tableName);
    const commonColumns = targetColumns.filter(tc => cloneColumns.some(cc => cc.name === tc.name && !tc.isComputed && !cc.isComputed));

    const originalPkColumns = pkColumns;
    // Filtrar pkColumns para excluir columnas computadas
    pkColumns = pkColumns.filter(pk => {
      const col = cloneColumns.find(c => c.name === pk);
      return col && !col.isComputed;
    });
    // Excluir columnas computadas conocidas que no se detectaron
    if (tableName === 'saArtPrecio') {
      pkColumns = pkColumns.filter(pk => pk !== 'co_alma_calculado');
    }

    const minLengths = {};
    for (const col of commonColumns) {
      const targetCol = targetColumns.find(tc => tc.name === col.name);
      const cloneCol = cloneColumns.find(cc => cc.name === col.name);
      const targetLen = (targetCol && targetCol.maxLength && targetCol.maxLength > 0) ? targetCol.maxLength : 1000;
      const cloneLen = (cloneCol && cloneCol.maxLength && cloneCol.maxLength > 0) ? cloneCol.maxLength : 1000;
      minLengths[col.name] = Math.min(targetLen, cloneLen);
    }

    const targetChecksum = await getChecksumWithColumns(targetPool, tableName, pkColumns, commonColumns, minLengths);
    const cloneChecksum = await getChecksumWithColumns(clonePool, tableName, pkColumns, commonColumns, minLengths);

    logger.debug(`Checksums - Target: ${targetChecksum}, Clone: ${cloneChecksum}`);

    if (targetChecksum === cloneChecksum) {
      logger.info(`Tabla ${tableName} no presentó cambios (checksums iguales).`);
      return true;
    } else {
      logger.warn(`Tabla ${tableName} presentó cambios (checksums diferentes).`);
      if (sync) {
        await syncTable(tableName, originalPkColumns);
        // Recalcular checksums después de sincronización
        const newTargetChecksum = await getChecksumWithColumns(targetPool, tableName, pkColumns, commonColumns, minLengths);
        const newCloneChecksum = await getChecksumWithColumns(clonePool, tableName, pkColumns, commonColumns, minLengths);
        if (newTargetChecksum === newCloneChecksum) {
          logger.info(`Tabla ${tableName} sincronizada exitosamente.`);
        } else {
          logger.error(`Tabla ${tableName} aún no sincronizada después de la operación.`);
        }
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