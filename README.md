# Script de Comparación y Sincronización de Tablas SQL Server

Este script permite comparar una tabla entre un servidor "target" (solo lectura) y un servidor "clone" (acceso completo), verificando si están sincronizadas usando checksums. Si no lo están, puede sincronizar automáticamente copiando datos de target a clone usando MERGE para upsert.

## Requisitos

- Node.js instalado.
- Acceso a dos servidores SQL Server: target (solo lectura) y clone (lectura/escritura).
- Las tablas deben tener una clave primaria definida en `tables.json`.

## Instalación

1. Clona el repositorio:
   ```bash
   git clone https://github.com/franklynortega/pclone
   cd pclone
   ```

2. Instala dependencias:
   ```bash
   npm install
   ```

3. Configura variables de entorno:
   ```bash
   cp .env.example .env
   # Edita .env con tus credenciales
   ```

4. Los logs se guardan en `sync.log` y se muestran en consola.

## Configuración

Edita las variables de entorno o modifica `config.js` con las credenciales reales:

- `TARGET_SERVER`: Servidor target.
- `TARGET_DB`: Base de datos target.
- `TARGET_USER`: Usuario target (solo lectura).
- `TARGET_PASS`: Contraseña target.

- `CLONE_SERVER`: Servidor clone.
- `CLONE_DB`: Base de datos clone.
- `CLONE_USER`: Usuario clone (con permisos de escritura).
- `CLONE_PASS`: Contraseña clone.

Ejemplo de ejecución con variables de entorno:

```bash
export TARGET_SERVER=server1
export TARGET_DB=myDB
# ... otros
node index.js MiTabla
```

## Configuración de Tablas

Edita `tables.json` con la lista de tablas a sincronizar, sus columnas de clave primaria, intervalo de sincronización en minutos y prioridad de ejecución:

```json
[
  {
    "table": "Usuarios",
    "pk": "UsuarioID",
    "syncIntervalMinutes": 5,
    "priority": 1
  },
  {
    "table": "Productos",
    "pk": "ProductoID",
    "syncIntervalMinutes": 30,
    "priority": 1
  },
  {
    "table": "Pedidos",
    "pk": "PedidoID",
    "syncIntervalMinutes": 60,
    "priority": 2
  }
]
```

- `syncIntervalMinutes`: Minutos entre verificaciones en el cron job.
- `priority`: Número para orden de ejecución (menor = primero, para tablas base antes de dependientes).

## Uso

### Verificar sincronización de una tabla específica

```bash
node index.js MiTabla --sync
```

Usa `PK_COLUMN=id` si no está en tables.json.

### Verificar y sincronizar todas las tablas

```bash
node index.js --sync
```

Procesa todas las tablas desde `tables.json`.

### Sincronización automática (para cron)

```bash
node sync-cron.js
```

Sincroniza todas las tablas automáticamente.

#### Configuración de Cron Job

El script usa lock files (`sync.lock`) para evitar ejecuciones concurrentes. Configura el cron externo con la frecuencia mínima de tus tablas (ej. cada 5 min si hay tablas con 5 min intervalo). Si configuras frecuencia mayor, algunas tablas se sincronizarán menos.

- **En Linux/macOS**: Agrega a crontab (`crontab -e`):
  ```
  */5 * * * * cd /path/to/project && node sync-cron.js
  ```
  (Ejecuta cada 5 minutos; ajusta al mínimo intervalo de tus tablas).

- **En Windows (Task Scheduler)**:
  1. Abre Task Scheduler (busca "Task Scheduler" en el menú Inicio).
  2. Haz clic en "Create Basic Task" (o "Create Task" para más opciones).
  3. Nombre: "Sync DB Tables".
  4. Trigger: "On a schedule", elige "Repeat task every" y ajusta al intervalo mínimo de tus tablas (ej. 5 minutes).
  5. Action: "Start a program".
     - Program/script: `C:\Windows\System32\cmd.exe`
     - Add arguments (opcional): `/c cd /d "C:\path\to\your\project" && node sync-cron.js`
     - O configura el "Start in" directory en la pestaña adicional.
  6. Configura condiciones (ej. solo si conectado a red).
  7. Guarda y prueba ejecutando manualmente.

## Notas

- Las claves primarias se definen en `tables.json`. Para una tabla específica, usa `PK_COLUMN` env var.
- Para tablas relacionadas, ordena `tables.json` por dependencias (padres primero).
- Maneja tipos de datos básicos; para tipos complejos, ajustar escaping en el código.
- No elimina filas en clone que no estén en target (solo upsert).
- Reintentos automáticos: Hasta 3 reintentos con backoff exponencial para errores de conexión o queries.

## Limitaciones

- No maneja deletes en clone.
- Batch size fijo en 100; ajusta para rendimiento.
- Requiere que las tablas tengan la misma estructura.

Si necesitas más funcionalidades, edita el código.