import sql from 'mssql';

const targetConfig = {
  server: process.env.TARGET_SERVER || 'localhost',
  database: process.env.TARGET_DB || 'targetDB',
  user: process.env.TARGET_USER || 'readonlyuser',
  password: process.env.TARGET_PASS || 'password',
  options: {
    encrypt: false, // Cambia a true si usas SSL
    trustServerCertificate: true
  }
};

const cloneConfig = {
  server: process.env.CLONE_SERVER || 'localhost',
  database: process.env.CLONE_DB || 'cloneDB',
  user: process.env.CLONE_USER || 'adminuser',
  password: process.env.CLONE_PASS || 'password',
  options: {
    encrypt: false,
    trustServerCertificate: true
  }
};

export { targetConfig, cloneConfig };