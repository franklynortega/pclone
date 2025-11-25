import sql from 'mssql';

const getConfig = (overrides = {}) => {
  // Helper to get value from overrides or env
  const getVal = (key, envKey, defaultVal) => {
    if (overrides[key] !== undefined) return overrides[key];
    if (process.env[envKey] !== undefined) return process.env[envKey];
    return defaultVal;
  };

  let targetServer = getVal('targetServer', 'TARGET_SERVER', 'localhost');
  const targetPort = parseInt(getVal('targetPort', 'TARGET_PORT', 1433));
  const targetDb = getVal('targetDb', 'TARGET_DB', 'targetDB');
  const targetUser = getVal('targetUser', 'TARGET_USER', 'readonlyuser');
  const targetPass = getVal('targetPass', 'TARGET_PASS', 'password');

  // Remove instance name from server if present (e.g., "server\SQLEXPRESS" -> "server")
  targetServer = targetServer.split('\\')[0];

  const targetConfig = {
    server: targetServer,
    port: targetPort,
    database: targetDb,
    user: targetUser,
    password: targetPass,
    options: {
      encrypt: false,
      trustServerCertificate: true
    }
  };

  let cloneServer = getVal('cloneServer', 'CLONE_SERVER', 'localhost');
  const clonePort = parseInt(getVal('clonePort', 'CLONE_PORT', 1433));
  const cloneDb = getVal('cloneDb', 'CLONE_DB', 'cloneDB');
  const cloneUser = getVal('cloneUser', 'CLONE_USER', 'adminuser');
  const clonePass = getVal('clonePass', 'CLONE_PASS', 'password');

  // Remove instance name from server if present
  cloneServer = cloneServer.split('\\')[0];

  const cloneConfig = {
    server: cloneServer,
    port: clonePort,
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