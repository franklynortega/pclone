import sql from 'mssql';

const getConfig = (overrides = {}) => {
  // Helper to get value from overrides or env
  const getVal = (key, envKey, defaultVal) => {
    if (overrides[key] !== undefined) return overrides[key];
    if (process.env[envKey] !== undefined) return process.env[envKey];
    return defaultVal;
  };

  const targetServer = getVal('targetServer', 'TARGET_SERVER', 'localhost');
  const targetPort = parseInt(getVal('targetPort', 'TARGET_PORT', 1433));
  const targetDb = getVal('targetDb', 'TARGET_DB', 'targetDB');
  const targetUser = getVal('targetUser', 'TARGET_USER', 'readonlyuser');
  const targetPass = getVal('targetPass', 'TARGET_PASS', 'password');

  const targetConfig = targetServer.includes('\\') ? {
    server: targetServer,
    port: targetPort,
    database: targetDb,
    user: targetUser,
    password: targetPass,
    options: {
      encrypt: false, // Cambia a true si usas SSL
      trustServerCertificate: true
    }
  } : {
    server: `${targetServer},${targetPort}`,
    database: targetDb,
    user: targetUser,
    password: targetPass,
    options: {
      encrypt: false, // Cambia a true si usas SSL
      trustServerCertificate: true
    }
  };

  const cloneServer = getVal('cloneServer', 'CLONE_SERVER', 'localhost');
  const clonePort = parseInt(getVal('clonePort', 'CLONE_PORT', 1433));
  const cloneDb = getVal('cloneDb', 'CLONE_DB', 'cloneDB');
  const cloneUser = getVal('cloneUser', 'CLONE_USER', 'adminuser');
  const clonePass = getVal('clonePass', 'CLONE_PASS', 'password');

  const cloneConfig = cloneServer.includes('\\') ? {
    server: cloneServer,
    port: clonePort,
    database: cloneDb,
    user: cloneUser,
    password: clonePass,
    options: {
      encrypt: false,
      trustServerCertificate: true
    }
  } : {
    server: `${cloneServer},${clonePort}`,
    database: cloneDb,
    user: cloneUser,
    password: clonePass,
    options: {
      encrypt: false,
      trustServerCertificate: true
    }
  };

  return { targetConfig, cloneConfig };
};

export { getConfig };