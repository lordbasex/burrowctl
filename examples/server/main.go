package main

import (
	"context"
	"log"

	"github.com/lordbasex/burrowctl/server"
)

func main() {
	// Load configuration from flags and environment variables
	config := server.LoadConfigFromFlags()

	// Override specific values for this example (keeping the detailed documentation)
	// The configuration below shows advanced examples but will be overridden by environment variables
	// _ = &server.ServerConfig{
	// 	// Device and connection configuration
	// 	DeviceID: "my-custom-device",
	// 	AMQPURL:  "amqp://burrowuser:burrowpass123@localhost:5672/",
	// 	MySQLDSN: "burrowuser:burrowpass123@tcp(localhost:3306)/burrowdb",

	// 	// RabbitMQ Reconnection configuration
	// 	// Esta configuración controla el comportamiento de reconexión automática del cliente
	// 	// cuando se pierde la conexión con RabbitMQ. Es especialmente útil en entornos
	// 	// donde la conectividad de red puede ser inestable o cuando el servidor RabbitMQ
	// 	// se reinicia. El cliente intentará reconectarse automáticamente con backoff exponencial.
	// 	// IMPORTANTE: MaxAttempts = 0 significa reconexión infinita (recomendado para producción).
	// 	// Esta configuración es ideal para períodos largos sin internet (horas o días).
	// 	//
	// 	// PROGRESIÓN DE INTENTOS CON LA CONFIGURACIÓN ACTUAL:
	// 	// ┌─────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Intento │ Tiempo Espera   │ Tiempo Acumulado│ Estado          │
	// 	// ├─────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │    1    │      2s         │       2s        │ Intento inicial │
	// 	// │    2    │      3s         │       5s        │ Backoff 1.5x    │
	// 	// │    3    │    4.5s         │     9.5s        │ Backoff 1.5x    │
	// 	// │    4    │   6.75s         │    16.25s       │ Backoff 1.5x    │
	// 	// │    5    │  10.125s        │    26.375s      │ Backoff 1.5x    │
	// 	// │    6    │  15.187s        │    41.562s      │ Backoff 1.5x    │
	// 	// │    7    │  22.781s        │    64.343s      │ Backoff 1.5x    │
	// 	// │    8    │  34.172s        │    98.515s      │ Backoff 1.5x    │
	// 	// │    9    │  51.258s        │   149.773s      │ Backoff 1.5x    │
	// 	// │   10    │  76.887s        │   226.66s       │ Backoff 1.5x    │
	// 	// │   11+   │    120s         │   Máximo 2min   │ Límite alcanzado│
	// 	// └─────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// ESCENARIOS DE USO:
	// 	// • 1 hora sin internet: 30+ intentos, máximo 2min entre intentos
	// 	// • 10 horas sin internet: 300+ intentos, máximo 2min entre intentos
	// 	// • 1 día sin internet: 720+ intentos, máximo 2min entre intentos
	// 	// • Cualquier duración: NUNCA se rinde, reconexión infinita
	// 	ReconnectEnabled:           true,
	// 	ReconnectMaxAttempts:       0,                 // 0 = Reconexión infinita (nunca se rinde)
	// 	ReconnectInitialInterval:   2 * time.Second,   // Empieza con 2 segundos
	// 	ReconnectMaxInterval:       120 * time.Second, // Máximo 2 minutos (ideal para períodos largos)
	// 	ReconnectBackoffMultiplier: 1.5,               // Multiplicador personalizado
	// 	ReconnectResetInterval:     10 * time.Minute,  // Reset después de 10 min de éxito

	// 	// Query Cache configuration
	// 	// El cache de queries mejora significativamente el rendimiento al almacenar
	// 	// resultados de consultas SQL frecuentes en memoria. Reduce la carga en la
	// 	// base de datos y acelera las respuestas para queries repetidas. El cache
	// 	// se limpia automáticamente para evitar el uso excesivo de memoria.
	// 	//
	// 	// FUNCIONAMIENTO DEL CACHE:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Query Recibida  │ Cache Hit/Miss  │ Acción          │ Tiempo Respuesta│
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT * FROM   │     MISS        │ Ejecuta en DB   │     ~50ms       │
	// 	// │ users WHERE id=1│                 │ + Guarda en     │                 │
	// 	// │                 │                 │   cache         │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT * FROM   │      HIT        │ Retorna desde   │     ~2ms        │
	// 	// │ users WHERE id=1│                 │   cache         │                 │
	// 	// │                 │                 │ (sin DB)        │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT * FROM   │     MISS        │ Ejecuta en DB   │     ~50ms       │
	// 	// │ users WHERE id=2│                 │ + Guarda en     │                 │
	// 	// │                 │                 │   cache         │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CONFIGURACIÓN ACTUAL:
	// 	// • Cache Size: 3000 queries (máximo en memoria)
	// 	// • TTL: 20 minutos (tiempo de vida de cada entrada)
	// 	// • Cleanup: 8 minutos (limpieza automática)
	// 	// • Beneficio: 25x más rápido para queries repetidas
	// 	//
	// 	// CONSUMO DE MEMORIA DEL CACHE (3000 queries):
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Tamaño Query    │ Registros por   │ Memoria por     │ Memoria Total   │
	// 	// │ Resultado       │ Query           │ Query           │ (3000 queries)  │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Query pequeña   │ 1-10 registros  │ ~2-5 KB         │ ~6-15 MB        │
	// 	// │ (configuración) │                 │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Query mediana   │ 10-100 registros│ ~5-50 KB        │ ~15-150 MB      │
	// 	// │ (lookup tables) │                 │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Query grande    │ 100-1000 reg.   │ ~50-500 KB      │ ~150-1.5 GB     │
	// 	// │ (reportes)      │                 │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Query muy grande│ 1000+ registros │ ~500 KB-5 MB    │ ~1.5-15 GB      │
	// 	// │ (datasets)      │                 │                 │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// EJEMPLOS PRÁCTICOS:
	// 	// • SELECT config: ~2 KB × 3000 = ~6 MB (muy eficiente)
	// 	// • SELECT users: ~20 KB × 3000 = ~60 MB (eficiente)
	// 	// • SELECT reports: ~200 KB × 3000 = ~600 MB (moderado)
	// 	// • SELECT datasets: ~2 MB × 3000 = ~6 GB (alto consumo)
	// 	//
	// 	// RECOMENDACIÓN: Para la mayoría de casos, 3000 queries
	// 	// consume entre 50-500 MB de memoria, que es muy razonable.
	// 	// El cache se limpia automáticamente cada 8 minutos.
	// 	//
	// 	// MECANISMOS DE LIMPIEZA DEL CACHE:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Mecanismo       │ Frecuencia      │ Qué Elimina     │ Cuándo          │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ CacheTTL        │ En cada acceso  │ Entrada expirada│ Al acceder a    │
	// 	// │ (20 min)        │ a la entrada    │ individual      │ entrada del     │
	// 	// │                 │                 │                 │ cache           │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ CacheCleanup    │ Cada 8 minutos  │ Todas las       │ Automáticamente │
	// 	// │ (8 min)         │ (programado)    │ entradas        │ en background   │
	// 	// │                 │                 │ huérfanas       │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CICLO DE VIDA DE UNA ENTRADA EN CACHE:
	// 	// ┌─────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Tiempo  │ Estado          │ Acción          │ Memoria         │
	// 	// ├─────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ 0 min   │ Query ejecutada │ Se guarda en    │ +2-5 KB         │
	// 	// │         │                 │ cache           │                 │
	// 	// ├─────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ 5 min   │ Query repetida  │ Se sirve desde  │ Sin cambio      │
	// 	// │         │                 │ cache (rápido)  │                 │
	// 	// ├─────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ 8 min   │ Cleanup ejecuta │ Elimina otras   │ -entradas       │
	// 	// │         │                 │ entradas exp.   │ huérfanas       │
	// 	// ├─────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ 15 min  │ Query repetida  │ Se sirve desde  │ Sin cambio      │
	// 	// │         │                 │ cache (rápido)  │                 │
	// 	// ├─────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ 20 min  │ TTL expira      │ Se elimina al   │ -2-5 KB         │
	// 	// │         │                 │ próximo acceso  │                 │
	// 	// └─────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// BENEFICIOS DE ESTE SISTEMA:
	// 	// • CacheTTL: Garantiza datos frescos (máximo 20 min de antigüedad)
	// 	// • CacheCleanup: Libera memoria automáticamente (cada 8 min)
	// 	// • Sin entradas huérfanas: Limpieza proactiva
	// 	// • Rendimiento óptimo: Datos frescos + memoria controlada
	// 	//
	// 	// ESCENARIOS DE USO:
	// 	// • Queries frecuentes: SELECT de configuración, lookup tables
	// 	// • Reportes repetidos: Estadísticas, dashboards
	// 	// • Datos estáticos: Catálogos, referencias
	// 	// • Reducción de carga: Menos conexiones a la base de datos
	// 	//
	// 	// QUERIES AFECTADAS POR EL CACHE:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Tipo de Query   │ Se Cachea       │ Ejemplo         │ Razón           │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT simple   │      ✅         │ SELECT * FROM   │ Datos estáticos │
	// 	// │                 │                 │ users           │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT con WHERE│      ✅         │ SELECT * FROM   │ Lookups         │
	// 	// │                 │                 │ users WHERE     │ frecuentes      │
	// 	// │                 │                 │ id = 1          │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT con JOIN │      ✅         │ SELECT u.*, p.* │ Consultas       │
	// 	// │                 │                 │ FROM users u    │ complejas       │
	// 	// │                 │                 │ JOIN profiles p │ repetidas       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ INSERT/UPDATE   │      ❌         │ INSERT INTO     │ Modifica datos  │
	// 	// │                 │                 │ users (...)     │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ DELETE          │      ❌         │ DELETE FROM     │ Modifica datos  │
	// 	// │                 │                 │ users WHERE...  │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ DDL (CREATE/    │      ❌         │ CREATE TABLE    │ Estructura DB   │
	// 	// │ ALTER/DROP)     │                 │ users (...)     │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// NOTA: Solo las queries SELECT se cachean. Las queries que modifican
	// 	// datos (INSERT, UPDATE, DELETE, DDL) NO se cachean por seguridad.
	// 	CacheEnabled: true,
	// 	CacheSize:    3000,             // Cache personalizado
	// 	CacheTTL:     20 * time.Minute, // TTL personalizado
	// 	CacheCleanup: 8 * time.Minute,  // Limpieza personalizada

	// 	// SQL Validation configuration
	// 	// La validación SQL proporciona una capa de seguridad adicional al analizar
	// 	// y validar todas las consultas antes de su ejecución. Previene inyecciones
	// 	// SQL, controla qué tipos de comandos están permitidos, y registra intentos
	// 	// de violación de seguridad para auditoría y monitoreo.
	// 	//
	// 	// NIVELES DE SEGURIDAD Y COMANDOS PERMITIDOS:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Tipo de Comando │ Permitido       │ Ejemplo         │ Uso Típico      │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT          │      ✅         │ SELECT * FROM   │ Consultas       │
	// 	// │                 │                 │ users           │ de lectura      │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ INSERT          │      ✅         │ INSERT INTO     │ Inserción de    │
	// 	// │                 │                 │ users (...)     │ datos           │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ UPDATE          │      ✅         │ UPDATE users    │ Modificación    │
	// 	// │                 │                 │ SET name = ?    │ de datos        │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ DELETE          │      ✅         │ DELETE FROM     │ Eliminación     │
	// 	// │                 │                 │ users WHERE...  │ de datos        │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ CREATE TABLE    │      ❌         │ CREATE TABLE    │ Estructura DB   │
	// 	// │                 │                 │ users (...)     │ (bloqueado)     │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ DROP TABLE      │      ❌         │ DROP TABLE      │ Estructura DB   │
	// 	// │                 │                 │ users           │ (bloqueado)     │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ ALTER TABLE     │      ❌         │ ALTER TABLE     │ Estructura DB   │
	// 	// │                 │                 │ users ADD...    │ (bloqueado)     │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ CALL/EXECUTE    │      ❌         │ CALL procedure  │ Stored Procs    │
	// 	// │                 │                 │ (param)         │ (bloqueado)     │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CONFIGURACIÓN ACTUAL:
	// 	// • ValidationEnabled: true (validación activa)
	// 	// • StrictMode: false (modo permisivo)
	// 	// • AllowDDL: false (no permite cambios de estructura)
	// 	// • AllowDML: true (permite modificación de datos)
	// 	// • AllowStoredProcs: false (no permite stored procedures)
	// 	// • MaxQueryLength: 8000 caracteres (límite de longitud)
	// 	// • LogViolations: true (registra intentos de violación)
	// 	//
	// 	// PROTECCIÓN CONTRA INYECCIÓN SQL:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Query Maliciosa │ Detección       │ Acción          │ Resultado       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT * FROM   │     ❌          │ Bloqueada       │ Error de        │
	// 	// │ users; DROP     │                 │ automáticamente │ validación      │
	// 	// │ TABLE users;    │                 │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT * FROM   │     ❌          │ Bloqueada       │ Error de        │
	// 	// │ users WHERE     │                 │ automáticamente │ validación      │
	// 	// │ id = 1 OR 1=1   │                 │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ SELECT * FROM   │     ✅          │ Permitida       │ Ejecución       │
	// 	// │ users WHERE     │                 │ (con parámetros │ normal          │
	// 	// │ id = ?          │                 │ seguros)        │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// BENEFICIOS DE SEGURIDAD:
	// 	// • Prevención de inyección SQL: Bloquea queries maliciosas
	// 	// • Control de acceso: Solo comandos permitidos
	// 	// • Auditoría completa: Registra todos los intentos
	// 	// • Cumplimiento: Cumple estándares de seguridad
	// 	ValidationEnabled: true,
	// 	StrictMode:        false, // Modo no estricto
	// 	AllowDDL:          false, // No permitir DDL
	// 	AllowDML:          true,  // Permitir DML
	// 	AllowStoredProcs:  false, // No permitir stored procedures
	// 	MaxQueryLength:    8000,  // Longitud máxima personalizada
	// 	LogViolations:     true,

	// 	// Performance configuration
	// 	// Esta configuración optimiza el rendimiento del servidor para manejar
	// 	// múltiples conexiones concurrentes y procesar requests de manera eficiente.
	// 	// Los workers procesan requests en paralelo, mientras que el rate limiting
	// 	// protege contra sobrecarga del sistema y ataques de denegación de servicio.
	// 	//
	// 	// ARQUITECTURA DE PROCESAMIENTO:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Componente      │ Cantidad        │ Función         │ Rendimiento     │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Workers         │ 30 goroutines   │ Procesan        │ 30 requests     │
	// 	// │                 │                 │ requests en     │ simultáneos     │
	// 	// │                 │                 │ paralelo        │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Queue Buffer    │ 1500 requests   │ Almacena        │ Evita pérdida   │
	// 	// │                 │                 │ requests        │ de requests     │
	// 	// │                 │                 │ pendientes      │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Rate Limiter    │ 150 req/s       │ Controla        │ Protege contra  │
	// 	// │                 │                 │ velocidad       │ sobrecarga      │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Burst Size      │ 300 requests    │ Permite picos   │ Flexibilidad    │
	// 	// │                 │                 │ temporales      │ en tráfico      │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// ESCENARIOS DE CARGA Y RENDIMIENTO:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Escenario       │ Requests/s      │ Workers         │ Latencia        │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Carga baja      │ 10-50 req/s     │ 10-15 workers   │ <50ms           │
	// 	// │                 │                 │ activos         │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Carga normal    │ 50-150 req/s    │ 15-25 workers   │ 50-100ms        │
	// 	// │                 │                 │ activos         │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Carga alta      │ 150-300 req/s   │ 25-30 workers   │ 100-200ms       │
	// 	// │                 │ (burst)         │ activos         │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Sobre carga     │ >300 req/s      │ Rate limited    │ 429 (Too Many)  │
	// 	// │                 │                 │ (protegido)     │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CONFIGURACIÓN ACTUAL:
	// 	// • Workers: 30 goroutines (procesamiento paralelo)
	// 	// • QueueSize: 1500 requests (buffer de espera)
	// 	// • RateLimit: 150 requests/segundo (control de velocidad)
	// 	// • BurstSize: 300 requests (permite picos temporales)
	// 	//
	// 	// BENEFICIOS DE RENDIMIENTO:
	// 	// • Procesamiento paralelo: 30 requests simultáneos
	// 	// • Buffer de espera: Evita pérdida de requests
	// 	// • Rate limiting: Protege contra sobrecarga
	// 	// • Burst handling: Maneja picos de tráfico
	// 	Workers:   30,   // Workers personalizados
	// 	QueueSize: 1500, // Tamaño de cola personalizado
	// 	RateLimit: 150,  // Rate limit personalizado
	// 	BurstSize: 300,  // Burst size personalizado

	// 	// Database configuration
	// 	// La configuración de la base de datos optimiza el pool de conexiones
	// 	// para maximizar el rendimiento y minimizar el uso de recursos. Controla
	// 	// cuántas conexiones se mantienen abiertas, cuántas están disponibles
	// 	// para uso inmediato, y cuánto tiempo pueden permanecer activas.
	// 	//
	// 	// ARQUITECTURA DEL POOL DE CONEXIONES:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Tipo Conexión   │ Cantidad        │ Estado          │ Uso             │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Conexiones      │ 30 conexiones   │ Idle (listas)   │ Uso inmediato   │
	// 	// │ Idle            │                 │ para usar       │ sin overhead    │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Conexiones      │ 80 conexiones   │ Máximo total    │ Escala según    │
	// 	// │ Máximas         │                 │ permitidas      │ demanda         │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Lifetime        │ 12 minutos      │ Tiempo máximo   │ Evita conexiones│
	// 	// │                 │                 │ por conexión    │ obsoletas       │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// ESCENARIOS DE USO Y RENDIMIENTO:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Escenario       │ Conexiones      │ Tiempo Respuesta│ Eficiencia      │
	// 	// │                 │ Activas         │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Carga baja      │ 5-15 conexiones │ <10ms           │ Muy eficiente   │
	// 	// │                 │ (idle ready)    │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Carga normal    │ 15-50 conexiones│ 10-50ms         │ Eficiente       │
	// 	// │                 │ (mixed)         │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Carga alta      │ 50-80 conexiones│ 50-200ms        │ Moderado        │
	// 	// │                 │ (near max)      │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Sobre carga     │ >80 conexiones  │ Queue/wait      │ Protegido       │
	// 	// │                 │ (rate limited)  │                 │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CONFIGURACIÓN ACTUAL:
	// 	// • PoolIdle: 30 conexiones (listas para uso inmediato)
	// 	// • PoolOpen: 80 conexiones (máximo total permitidas)
	// 	// • ConnLifetime: 12 minutos (tiempo máximo por conexión)
	// 	//
	// 	// BENEFICIOS DEL POOL DE CONEXIONES:
	// 	// • Conexiones reutilizables: Evita overhead de crear/cerrar
	// 	// • Escalabilidad automática: Se adapta a la demanda
	// 	// • Protección contra sobrecarga: Límite máximo de conexiones
	// 	// • Limpieza automática: Conexiones se renuevan cada 12 min
	// 	//
	// 	// GESTIÓN DE RECURSOS:
	// 	// • Memoria por conexión: ~1-2 MB (depende del driver)
	// 	// • Memoria total idle: ~30-60 MB (30 conexiones × 1-2 MB)
	// 	// • Memoria máxima: ~80-160 MB (80 conexiones × 1-2 MB)
	// 	// • Renovación automática: Cada 12 minutos
	// 	PoolIdle:     30,               // Conexiones idle personalizadas
	// 	PoolOpen:     80,               // Conexiones abiertas personalizadas
	// 	ConnLifetime: 12 * time.Minute, // Lifetime personalizado

	// 	// Monitoring configuration
	// 	// El sistema de monitoreo proporciona métricas en tiempo real sobre
	// 	// el rendimiento del servidor, incluyendo estadísticas de queries,
	// 	// uso de recursos, y estado de las conexiones. Es esencial para
	// 	// el mantenimiento proactivo y la optimización del rendimiento.
	// 	//
	// 	// MÉTRICAS MONITOREADAS EN TIEMPO REAL:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Categoría       │ Métricas        │ Frecuencia      │ Uso             │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Rendimiento     │ Requests/s      │ Cada 45s        │ Análisis de     │
	// 	// │                 │ Latencia P95    │                 │ rendimiento     │
	// 	// │                 │ Throughput      │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Recursos        │ CPU Usage       │ Cada 45s        │ Detección de    │
	// 	// │                 │ Memory Usage    │                 │ cuellos de      │
	// 	// │                 │ Goroutines      │                 │ botella         │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Base de Datos   │ Active Conns    │ Cada 45s        │ Optimización    │
	// 	// │                 │ Idle Conns      │                 │ del pool        │
	// 	// │                 │ Query Time      │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Cache           │ Hit Rate        │ Cada 45s        │ Eficiencia del  │
	// 	// │                 │ Miss Rate       │                 │ cache           │
	// 	// │                 │ Size            │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Seguridad       │ Violations      │ Cada 45s        │ Detección de    │
	// 	// │                 │ Blocked Queries │                 │ ataques         │
	// 	// │                 │ Rate Limits     │                 │                 │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// EJEMPLOS DE REPORTES DE MONITOREO:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Métrica         │ Valor Actual    │ Umbral          │ Estado          │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Requests/s      │ 125 req/s       │ <150 req/s      │ ✅ Normal       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Latencia P95    │ 85ms            │ <100ms          │ ✅ Normal       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ CPU Usage       │ 45%             │ <80%            │ ✅ Normal       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Memory Usage    │ 180MB           │ <500MB          │ ✅ Normal       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Cache Hit Rate  │ 78%             │ >70%            │ ✅ Eficiente    │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Active Conns    │ 45/80           │ <80             │ ✅ Normal       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Security Viol.  │ 3 (última hora) │ <10/hour        │ ✅ Seguro       │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CONFIGURACIÓN ACTUAL:
	// 	// • MonitoringEnabled: true (monitoreo activo)
	// 	// • MonitoringInterval: 45 segundos (frecuencia de reportes)
	// 	//
	// 	// BENEFICIOS DEL MONITOREO:
	// 	// • Detección proactiva: Identifica problemas antes de que afecten
	// 	// • Optimización continua: Datos para mejorar rendimiento
	// 	// • Alertas automáticas: Notificaciones cuando se superan umbrales
	// 	// • Análisis histórico: Tendencias y patrones de uso
	// 	// • Cumplimiento: Registros para auditoría y compliance
	// 	MonitoringEnabled:  true,
	// 	MonitoringInterval: 45 * time.Second, // Intervalo personalizado

	// 	// Heartbeat configuration
	// 	// El sistema de heartbeat monitorea la conectividad entre clientes
	// 	// y servidor para detectar desconexiones rápidamente. Envía señales
	// 	// periódicas para verificar que la conexión esté activa y limpia
	// 	// automáticamente las conexiones de clientes que ya no responden.
	// 	//
	// 	// ARQUITECTURA DEL SISTEMA DE HEARTBEAT:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Componente      │ Frecuencia      │ Función         │ Estado          │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Cliente PING    │ Cada 15s        │ Envía señal     │ Activo          │
	// 	// │                 │                 │ de vida         │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Servidor PONG   │ Inmediato       │ Responde        │ Conectado       │
	// 	// │                 │ (<5s)           │ al PING         │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Timeout         │ 5s máximo       │ Detecta         │ Desconectado    │
	// 	// │                 │                 │ desconexión     │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Cleanup         │ Cada 1 min      │ Limpia clientes │ Mantiene        │
	// 	// │                 │                 │ inactivos       │ lista limpia    │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// ESCENARIOS DE CONECTIVIDAD Y DETECCIÓN:
	// 	// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
	// 	// │ Escenario       │ Tiempo          │ Acción          │ Resultado       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Conexión normal │ 0-5s            │ PING → PONG     │ ✅ Conectado    │
	// 	// │                 │                 │ inmediato       │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Latencia alta   │ 5-15s           │ PING → PONG     │ ⚠️ Lento pero   │
	// 	// │                 │                 │ tardío          │ conectado       │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Desconexión     │ >15s            │ PING → timeout  │ ❌ Desconectado │
	// 	// │ parcial         │                 │                 │                 │
	// 	// ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
	// 	// │ Desconexión     │ >1 min          │ Cleanup         │ 🗑️ Cliente      │
	// 	// │ total           │                 │ automático      │ eliminado       │
	// 	// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
	// 	//
	// 	// CONFIGURACIÓN ACTUAL:
	// 	// • HeartbeatInterval: 15 segundos (frecuencia de PING)
	// 	// • HeartbeatTimeout: 5 segundos (tiempo máximo de respuesta)
	// 	// • HeartbeatMaxMissed: 5 heartbeats (máximo perdidos antes de desconectar)
	// 	// • HeartbeatCleanup: 1 minuto (limpieza de clientes inactivos)
	// 	// • HeartbeatMaxClientAge: 2 minutos (edad máxima de registros de cliente)
	// 	//
	// 	// BENEFICIOS DEL SISTEMA DE HEARTBEAT:
	// 	// • Detección rápida: Identifica desconexiones en 15-20 segundos
	// 	// • Limpieza automática: Elimina clientes inactivos cada minuto
	// 	// • Recuperación automática: Clientes se reconectan automáticamente
	// 	// • Monitoreo en tiempo real: Estado de conectividad actualizado
	// 	// • Prevención de recursos huérfanos: Libera memoria automáticamente
	// 	//
	// 	// GESTIÓN DE RECURSOS:
	// 	// • Memoria por cliente: ~1-2 KB (registro de estado)
	// 	// • Overhead de red: ~100 bytes por heartbeat (mínimo)
	// 	// • CPU overhead: <1% (procesamiento de heartbeats)
	// 	// • Limpieza automática: Cada minuto libera memoria
	// 	HeartbeatEnabled:      true,
	// 	HeartbeatInterval:     15 * time.Second, // Intervalo personalizado
	// 	HeartbeatTimeout:      5 * time.Second,  // Timeout personalizado
	// 	HeartbeatMaxMissed:    5,                // Máximo de heartbeats perdidos
	// 	HeartbeatCleanup:      1 * time.Minute,  // Limpieza personalizada
	// 	HeartbeatMaxClientAge: 2 * time.Minute,  // Edad máxima del cliente
	// }

	// Note: The ServerConfig struct above is shown for documentation purposes
	// but config (loaded from environment variables) will be used instead

	// Create server factory with environment-loaded configuration
	factory := server.NewServerFactory(config)

	// Start the server with our custom configuration
	ctx := context.Background()
	if err := factory.StartServer(ctx); err != nil {
		log.Fatal("Server failed:", err)
	}
}
