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
  return sql.NVarChar;
}

async function compareTable(tableName, pkColumns, sync) {
  let targetPool, clonePool;
  try {
    const { targetConfig, cloneConfig } = getConfig();
    targetPool = await new sql.ConnectionPool(targetConfig).connect();
    clonePool = await new sql.ConnectionPool(cloneConfig).connect();

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
      return sql.NVarChar;
    }

    async function compareTable(tableName, pkColumns, sync) {
      let targetPool, clonePool;
      try {
        const { targetConfig, cloneConfig } = getConfig();
        targetPool = await new sql.ConnectionPool(targetConfig).connect();
        clonePool = await new sql.ConnectionPool(cloneConfig).connect();

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
            await syncTable(targetPool, clonePool, tableName, originalPkColumns);
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

    async function syncTable(targetPool, clonePool, tableName, pkColumns) {
      logger.info(`Iniciando sincronización de tabla ${tableName}...`);
      try {
        // 1. Obtener columnas
        const columns = await getColumns(targetPool, tableName);
        const columnNames = columns.map(c => `[${c.name}]`).join(', ');

        // 2. Leer datos de Target
        // Nota: Para tablas muy grandes esto debería ser paginado o stream, 
        // pero para esta implementación asumimos que cabe en memoria o es manejable.
        const request = targetPool.request();
        request.stream = true;
        request.query(`SELECT ${columnNames} FROM ${tableName}`);

        const table = new sql.Table(`#Temp_${tableName.replace(/[^a-zA-Z0-9_]/g, '')}`);
        table.create = true;

        columns.forEach(col => {
          let type;
          if (col.type.toLowerCase().includes('int')) type = sql.Int;
          else if (col.type.toLowerCase().includes('varchar')) type = sql.NVarChar(sql.MAX);
          else if (col.type.toLowerCase().includes('datetime')) type = sql.DateTime;
          else if (col.type.toLowerCase().includes('bit')) type = sql.Bit;
          else if (col.type.toLowerCase().includes('decimal')) type = sql.Decimal(18, 5); // Ajustar precisión según necesidad
          else type = sql.NVarChar(sql.MAX); // Fallback

          table.columns.add(col.name, type, { nullable: col.nullable });
        });

        const rows = [];

        return new Promise((resolve, reject) => {
          request.on('row', row => {
            // Convertir row object a array de valores en orden de columnas
            /*
            const rowValues = columns.map(col => row[col.name]);
            table.rows.add(...rowValues);
            */
            // Bulk insert requiere añadir filas a la tabla en memoria
            // sql.Table.rows.add espera argumentos posicionales
            // Es mejor acumular y hacer bulk insert por lotes si fuera necesario, 
            // pero mssql soporta table.rows.add

            // Mapeo simple de valores
            const values = [];
            for (const col of columns) {
              values.push(row[col.name]);
            }
            table.rows.add(...values);
          });

          request.on('error', err => {
            reject(err);
          });

          request.on('done', async () => {
            try {
              // 3. Bulk Insert a tabla temporal en Clone
              // Primero crear tabla temporal
              // La creación es automática si table.create = true en bulk, 
              // pero para tablas temporales (#) a veces es mejor crearla explícitamente si hay tipos complejos.
              // Intentaremos con el bulk directo.

              const cloneRequest = clonePool.request();

              // Crear tabla temporal manualmente para asegurar tipos
              const colDefs = columns.map(c => {
                let typeDef = c.type;
                if (c.maxLength && c.maxLength > 0 && (c.type.includes('char') || c.type.includes('binary'))) {
                  typeDef += `(${c.maxLength})`;
                } else if (c.maxLength === -1 && (c.type.includes('char') || c.type.includes('binary'))) {
                  typeDef += '(MAX)';
                }
                if (c.precision && c.scale && (c.type.includes('decimal') || c.type.includes('numeric'))) {
                  typeDef += `(${c.precision},${c.scale})`;
                }
                return `[${c.name}] ${typeDef}`;
              }).join(', ');

              const tempTableName = `#Temp_${tableName.replace(/[^a-zA-Z0-9_]/g, '')}`;
              await cloneRequest.query(`IF OBJECT_ID('tempdb..${tempTableName}') IS NOT NULL DROP TABLE ${tempTableName}; CREATE TABLE ${tempTableName} (${colDefs})`);

              // Bulk Insert
              // table.name = tempTableName (ya seteado arriba pero sin # si usamos create: false, pero aqui lo creamos manual)
              // Resetear table para bulk
              const bulkTable = new sql.Table(tempTableName);
              bulkTable.create = false; // Ya la creamos
              columns.forEach(col => {
                // Mapeo de tipos JS a SQL para el driver
                let type = sql.NVarChar(sql.MAX);
                const t = col.type.toLowerCase();
                if (t.includes('int')) type = sql.Int;
                else if (t.includes('bigint')) type = sql.BigInt;
                else if (t.includes('smallint')) type = sql.SmallInt;
                else if (t.includes('tinyint')) type = sql.TinyInt;
                else if (t.includes('bit')) type = sql.Bit;
                else if (t.includes('datetime')) type = sql.DateTime;
                else if (t.includes('date')) type = sql.Date;
                else if (t.includes('decimal') || t.includes('numeric')) type = sql.Decimal(col.precision, col.scale);
                else if (t.includes('float')) type = sql.Float;
                else if (t.includes('real')) type = sql.Real;
                else if (t.includes('binary') || t.includes('image')) type = sql.VarBinary(sql.MAX);

                bulkTable.columns.add(col.name, type, { nullable: true });
              });

              // Copiar filas de la tabla anterior a esta nueva instancia configurada
              table.rows.forEach(row => bulkTable.rows.add(...row));

              await cloneRequest.bulk(bulkTable);

              // 4. MERGE
              const pkMatch = pkColumns.map(pk => `T.[${pk}] = S.[${pk}]`).join(' AND ');
              const updateSet = columns.filter(c => !pkColumns.includes(c.name) && !c.isComputed).map(c => `T.[${c.name}] = S.[${c.name}]`).join(', ');
              const insertCols = columns.filter(c => !c.isComputed).map(c => `[${c.name}]`).join(', ');
              const insertVals = columns.filter(c => !c.isComputed).map(c => `S.[${c.name}]`).join(', ');

              const mergeQuery = `
            MERGE ${tableName} AS T
            USING ${tempTableName} AS S
            ON (${pkMatch})
            WHEN MATCHED THEN
              UPDATE SET ${updateSet}
            WHEN NOT MATCHED BY TARGET THEN
              INSERT (${insertCols}) VALUES (${insertVals})
            WHEN NOT MATCHED BY SOURCE THEN
              DELETE;
            DROP TABLE ${tempTableName};
          `;

              await cloneRequest.query(mergeQuery);
              logger.info(`Sincronización de tabla ${tableName} completada exitosamente.`);
              resolve(true);

            } catch (err) {
              if (type && type.toLowerCase().includes('nvarchar')) return sql.NText;
              if (type && type.toLowerCase().includes('datetime')) return sql.DateTime;
              return sql.NVarChar;
            }

            async function compareTable(tableName, pkColumns, sync) {
              let targetPool, clonePool;
              try {
                const { targetConfig, cloneConfig } = getConfig();
                targetPool = await new sql.ConnectionPool(targetConfig).connect();
                clonePool = await new sql.ConnectionPool(cloneConfig).connect();

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
                    await syncTable(targetPool, clonePool, tableName, originalPkColumns);
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

            async function syncTable(targetPool, clonePool, tableName, pkColumns) {
              logger.info(`Iniciando sincronización de tabla ${tableName}...`);
              try {
                // 1. Obtener columnas
                const columns = await getColumns(targetPool, tableName);
                const columnNames = columns.map(c => `[${c.name}]`).join(', ');

                // 2. Leer datos de Target
                // Nota: Para tablas muy grandes esto debería ser paginado o stream, 
                // pero para esta implementación asumimos que cabe en memoria o es manejable.
                const request = targetPool.request();
                request.stream = true;
                request.query(`SELECT ${columnNames} FROM ${tableName}`);

                const table = new sql.Table(`#Temp_${tableName.replace(/[^a-zA-Z0-9_]/g, '')}`);
                table.create = true;

                columns.forEach(col => {
                  let type;
                  if (col.type.toLowerCase().includes('int')) type = sql.Int;
                  else if (col.type.toLowerCase().includes('varchar')) type = sql.NVarChar(sql.MAX);
                  else if (col.type.toLowerCase().includes('datetime')) type = sql.DateTime;
                  else if (col.type.toLowerCase().includes('bit')) type = sql.Bit;
                  else if (col.type.toLowerCase().includes('decimal')) type = sql.Decimal(18, 5); // Ajustar precisión según necesidad
                  else type = sql.NVarChar(sql.MAX); // Fallback

                  table.columns.add(col.name, type, { nullable: col.nullable });
                });

                const rows = [];

                return new Promise((resolve, reject) => {
                  request.on('row', row => {
                    // Convertir row object a array de valores en orden de columnas
                    /*
                    const rowValues = columns.map(col => row[col.name]);
                    table.rows.add(...rowValues);
                    */
                    // Bulk insert requiere añadir filas a la tabla en memoria
                    // sql.Table.rows.add espera argumentos posicionales
                    // Es mejor acumular y hacer bulk insert por lotes si fuera necesario, 
                    // pero mssql soporta table.rows.add

                    // Mapeo simple de valores
                    const values = [];
                    for (const col of columns) {
                      values.push(row[col.name]);
                    }
                    table.rows.add(...values);
                  });

                  request.on('error', err => {
                    reject(err);
                  });

                  request.on('done', async () => {
                    try {
                      // 3. Bulk Insert a tabla temporal en Clone
                      // Primero crear tabla temporal
                      // La creación es automática si table.create = true en bulk, 
                      // pero para tablas temporales (#) a veces es mejor crearla explícitamente si hay tipos complejos.
                      // Intentaremos con el bulk directo.

                      const cloneRequest = clonePool.request();

                      // Crear tabla temporal manualmente para asegurar tipos
                      const colDefs = columns.map(c => {
                        let typeDef = c.type;
                        if (c.maxLength && c.maxLength > 0 && (c.type.includes('char') || c.type.includes('binary'))) {
                          typeDef += `(${c.maxLength})`;
                        } else if (c.maxLength === -1 && (c.type.includes('char') || c.type.includes('binary'))) {
                          typeDef += '(MAX)';
                        }
                        if (c.precision && c.scale && (c.type.includes('decimal') || c.type.includes('numeric'))) {
                          typeDef += `(${c.precision},${c.scale})`;
                        }
                        return `[${c.name}] ${typeDef}`;
                      }).join(', ');

                      const tempTableName = `#Temp_${tableName.replace(/[^a-zA-Z0-9_]/g, '')}`;
                      await cloneRequest.query(`IF OBJECT_ID('tempdb..${tempTableName}') IS NOT NULL DROP TABLE ${tempTableName}; CREATE TABLE ${tempTableName} (${colDefs})`);

                      // Bulk Insert
                      // table.name = tempTableName (ya seteado arriba pero sin # si usamos create: false, pero aqui lo creamos manual)
                      // Resetear table para bulk
                      const bulkTable = new sql.Table(tempTableName);
                      bulkTable.create = false; // Ya la creamos
                      columns.forEach(col => {
                        // Mapeo de tipos JS a SQL para el driver
                        let type = sql.NVarChar(sql.MAX);
                        const t = col.type.toLowerCase();
                        if (t.includes('int')) type = sql.Int;
                        else if (t.includes('bigint')) type = sql.BigInt;
                        else if (t.includes('smallint')) type = sql.SmallInt;
                        else if (t.includes('tinyint')) type = sql.TinyInt;
                        else if (t.includes('bit')) type = sql.Bit;
                        else if (t.includes('datetime')) type = sql.DateTime;
                        else if (t.includes('date')) type = sql.Date;
                        else if (t.includes('decimal') || t.includes('numeric')) type = sql.Decimal(col.precision, col.scale);
                        else if (t.includes('float')) type = sql.Float;
                        else if (t.includes('real')) type = sql.Real;
                        else if (t.includes('binary') || t.includes('image')) type = sql.VarBinary(sql.MAX);

                        bulkTable.columns.add(col.name, type, { nullable: true });
                      });

                      // Copiar filas de la tabla anterior a esta nueva instancia configurada
                      table.rows.forEach(row => bulkTable.rows.add(...row));

                      await cloneRequest.bulk(bulkTable);

                      // 4. MERGE
                      const pkMatch = pkColumns.map(pk => `T.[${pk}] = S.[${pk}]`).join(' AND ');
                      const updateSet = columns.filter(c => !pkColumns.includes(c.name) && !c.isComputed).map(c => `T.[${c.name}] = S.[${c.name}]`).join(', ');
                      const insertCols = columns.filter(c => !c.isComputed).map(c => `[${c.name}]`).join(', ');
                      const insertVals = columns.filter(c => !c.isComputed).map(c => `S.[${c.name}]`).join(', ');

                      const mergeQuery = `
            MERGE ${tableName} AS T
            USING ${tempTableName} AS S
            ON (${pkMatch})
            WHEN MATCHED THEN
              UPDATE SET ${updateSet}
            WHEN NOT MATCHED BY TARGET THEN
              INSERT (${insertCols}) VALUES (${insertVals})
            WHEN NOT MATCHED BY SOURCE THEN
              DELETE;
            DROP TABLE ${tempTableName};
          `;

                      await cloneRequest.query(mergeQuery);
                      logger.info(`Sincronización de tabla ${tableName} completada exitosamente.`);
                      resolve(true);

                    } catch (err) {
                      reject(err);
                    }
                  });
                });

              } catch (error) {
                logger.error(`Error sincronizando tabla ${tableName}:`, error);
                throw error;
              }
            }

            export { compareTable };