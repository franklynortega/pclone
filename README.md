# PClone - Interfaz Web de Sincronización de Bases de Datos SQL Server

PClone es una aplicación web completa para comparar y sincronizar tablas entre servidores SQL Server. Incluye una interfaz intuitiva para gestionar presets de configuración, programar tareas automáticas y monitorear el estado de sincronización en tiempo real.

## Características

- **Interfaz Web Intuitiva**: Gestiona sincronizaciones desde el navegador
- **Presets de Configuración**: Guarda y reutiliza configuraciones de sincronización
- **Programación Automática**: Crea trabajos cron para ejecuciones programadas
- **Monitoreo en Tiempo Real**: Visualiza logs y progreso de sincronización
- **Sincronización Segura**: Compara tablas usando checksums antes de actualizar
- **Soporte Multi-usuario**: Cada usuario tiene sus propios presets y trabajos
- **Responsive Design**: Funciona en desktop y dispositivos móviles

## Requisitos

- Node.js instalado (versión 18+)
- Acceso a dos servidores SQL Server: target (solo lectura) y clone (lectura/escritura)
- Navegador web moderno

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
   # Edita .env con tus credenciales de base de datos
   ```

4. Inicia el servidor:
   ```bash
   npm start
   ```

5. Abre tu navegador en `http://localhost:3000`

## Configuración Inicial

### Variables de Entorno (.env)

```env
PORT=3000
API_KEY=tu-api-key-segura
TARGET_SERVER=servidor-target
TARGET_DB=base-datos-target
TARGET_USER=usuario-target
TARGET_PASS=contraseña-target
CLONE_SERVER=servidor-clone
CLONE_DB=base-datos-clone
CLONE_USER=usuario-clone
CLONE_PASS=contraseña-clone
```

### Usuarios

Los usuarios se definen en `users.txt` con formato:
```
usuario:hash-sha256
```

Ejemplo:
```
admin:a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3
```

### Tablas por Defecto

Configura `tables.json` con las tablas disponibles para sincronizar:

```json
[
  {
    "table": "Usuarios",
    "pk": ["UsuarioID"]
  },
  {
    "table": "Productos",
    "pk": ["ProductoID"]
  }
]
```

## Uso de la Interfaz Web

### 1. Inicio de Sesión

- Accede a `http://localhost:3000`
- Ingresa tus credenciales definidas en `users.txt`

### 2. Gestión de Presets

#### Crear un Preset
1. Ve a la pestaña "Presets"
2. Haz clic en "Programar" para crear un nuevo preset
3. Configura:
   - **Servidor Target**: Servidor de origen (solo lectura)
   - **Base de Datos Target**: BD de origen
   - **Credenciales Target**: Usuario y contraseña
   - **Servidor Clone**: Servidor destino (lectura/escritura)
   - **Base de Datos Clone**: BD destino
   - **Credenciales Clone**: Usuario y contraseña
   - **Tablas**: Agrega tablas con sus claves primarias
   - **API Key**: Clave de API para autenticación
   - **Sincronizar**: Activa para actualizar datos, desactiva para solo comparar

#### Ejecutar un Preset
- En la lista de presets, haz clic en "Ejecutar"
- Los resultados se muestran en tiempo real en la sección de logs

#### Ejecutar Todos los Presets
- Haz clic en "Ejecutar Todos" para procesar todos los presets en secuencia

### 3. Programación Automática

#### Crear un Trabajo Programado
1. En la pestaña "Presets", haz clic en "Programar"
2. Selecciona un preset existente
3. Elige la frecuencia:
   - **Cada X minutos**: Especifica el intervalo
   - **Cada X horas**: Especifica el intervalo
   - **Diariamente**: Selecciona la hora
   - **Semanalmente**: Selecciona día y hora
   - **Mensualmente**: Selecciona día del mes y hora
4. El sistema genera automáticamente la expresión cron

#### Gestionar Trabajos Programados
- Ver trabajos activos en "Trabajos Programados"
- Detener trabajos con el botón "Detener"
- Los trabajos se ejecutan automáticamente según el horario configurado

### 4. Monitoreo

- **Logs en Tiempo Real**: Visualiza el progreso de cada tabla
- **Estados de Sincronización**: Verifica si las tablas están actualizadas
- **Historial de Ejecuciones**: Consulta logs anteriores en la consola del servidor

## Arquitectura del Sistema

### Backend (server.js)
- **API REST**: Endpoints para gestión de presets y sincronización
- **Autenticación**: Sistema de usuarios con hash SHA-256
- **Programación**: Integración con node-cron para trabajos automáticos
- **Procesamiento**: Lógica de comparación y sincronización de tablas

### Frontend (public/)
- **Interfaz Responsive**: Bootstrap 5 con diseño móvil
- **JavaScript Asíncrono**: Actualizaciones en tiempo real sin recargar
- **Gestión de Estado**: Manejo de sesiones y configuración del usuario

### Seguridad
- **Autenticación por Usuario**: Cada usuario tiene sus propios presets
- **API Keys**: Protección de endpoints con claves de API
- **Validación de Datos**: Verificación de entradas en frontend y backend

## Notas Técnicas

- Las claves primarias se definen en `tables.json`
- Soporta claves primarias compuestas
- Maneja tipos de datos básicos, incluyendo VARBINARY/IMAGE
- No elimina filas en clone que no estén en target (solo upsert)
- Reintentos automáticos con backoff exponencial

## Limitaciones

- No maneja deletes en clone
- Batch size fijo en 100 filas
- Requiere que las tablas tengan la misma estructura
- Trabajos cron se pierden al reiniciar el servidor (persistencia futura)

Si necesitas más funcionalidades, edita el código fuente.