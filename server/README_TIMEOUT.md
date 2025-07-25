# Configuración de Timeouts en el Servidor

## Problema Original

En versiones anteriores, varios timeouts estaban hardcodeados en el código:

```go
// ❌ ANTES - Timeouts hardcodeados
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)  // SQL
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)  // Commands
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)  // Functions
```

Esto presentaba varios problemas:
- **No configurable**: No se podía ajustar sin cambiar el código
- **No flexible**: Diferentes entornos necesitan diferentes timeouts
- **No consistente**: Otros timeouts en el sistema son configurables

## Tipos de Timeouts

### 1. **CommandTimeout** - Para comandos del sistema
```go
// Timeout para ejecución de comandos del sistema (ps, ls, etc.)
ctx, cancel := context.WithTimeout(context.Background(), h.commandTimeout)
```

**Propósito**: Controlar cuánto tiempo puede ejecutarse un comando del sistema
**Ejemplo**: `ps aux`, `ls -la`, `df -h`
**Valor típico**: 30-60 segundos
**Impacto**: Si se excede, mata el comando

### 2. **SQLTimeout** - Para consultas SQL
```go
// Timeout para ejecución de consultas SQL
ctx, cancel := context.WithTimeout(context.Background(), h.sqlTimeout)
```

**Propósito**: Controlar cuánto tiempo puede ejecutarse una consulta SQL
**Ejemplo**: `SELECT * FROM large_table`, `UPDATE millions_of_rows`
**Valor típico**: 30-300 segundos
**Impacto**: Si se excede, cancela la consulta

### 3. **FunctionTimeout** - Para funciones personalizadas
```go
// Timeout para ejecución de funciones personalizadas
ctx, cancel := context.WithTimeout(context.Background(), h.functionTimeout)
```

**Propósito**: Controlar cuánto tiempo puede ejecutarse una función
**Ejemplo**: `processData()`, `generateReport()`
**Valor típico**: 30-120 segundos
**Impacto**: Si se excede, cancela la función

### 4. **HeartbeatTimeout** - Para latidos de conexión
```go
// Timeout para respuestas de heartbeat entre cliente y servidor
ResponseTimeout: sc.HeartbeatTimeout,
```

**Propósito**: Controlar cuánto tiempo esperar una respuesta de heartbeat
**Ejemplo**: PING/PONG entre cliente y servidor
**Valor típico**: 2-10 segundos
**Impacto**: Si se excede, desconecta al cliente

## Diferencias Clave

| Aspecto | CommandTimeout | SQLTimeout | FunctionTimeout | HeartbeatTimeout |
|---------|----------------|------------|-----------------|------------------|
| **Propósito** | Comandos del sistema | Consultas SQL | Funciones personalizadas | Latidos de conexión |
| **Escala de tiempo** | Segundos a minutos | Segundos a minutos | Segundos a minutos | Milisegundos a segundos |
| **Frecuencia** | Por comando ejecutado | Por consulta ejecutada | Por función ejecutada | Continuo (cada pocos segundos) |
| **Impacto** | Mata el comando | Cancela la consulta | Cancela la función | Desconecta al cliente |
| **Ejemplo** | `sleep 100` se cancela después de 30s | `SELECT` complejo se cancela después de 60s | `processData()` se cancela después de 45s | Cliente se desconecta si no responde en 5s |

## Solución Implementada

### 1. Configuración en ServerConfig

Se agregaron nuevos campos a la estructura `ServerConfig`:

```go
type ServerConfig struct {
    // ... otros campos ...
    
    // Command execution configuration
    CommandTimeout time.Duration

    // Query execution configuration
    SQLTimeout     time.Duration
    FunctionTimeout time.Duration
}
```

### 2. Valores por Defecto

Los timeouts por defecto son de 30 segundos:

```go
func DefaultServerConfig() *ServerConfig {
    return &ServerConfig{
        // ... otros campos ...
        CommandTimeout: 30 * time.Second,
        SQLTimeout:     30 * time.Second,
        FunctionTimeout: 30 * time.Second,
    }
}
```

### 3. Configuración desde Línea de Comandos

Puedes configurar cada timeout independientemente:

```bash
# Configurar timeouts específicos
./server -command-timeout=60s -sql-timeout=120s -function-timeout=45s

# Configurar timeout de comandos largos
./server -command-timeout=5m

# Configurar timeout de SQL para consultas complejas
./server -sql-timeout=10m

# Configurar timeout de funciones para procesamiento
./server -function-timeout=2m
```

### 4. Configuración desde Variables de Entorno

También puedes usar variables de entorno:

```bash
# Configurar timeouts específicos
export COMMAND_TIMEOUT=60s
export SQL_TIMEOUT=120s
export FUNCTION_TIMEOUT=45s
./server

# Configurar timeout de comandos largos
export COMMAND_TIMEOUT=5m
./server
```

### 5. Uso en Código

#### Constructor Actualizado

El constructor `NewHandler` ahora acepta todos los timeouts como parámetros:

```go
// ✅ AHORA - Todos los timeouts configurable
handler := NewHandler(
    deviceID,
    amqpURL,
    mysqlDSN,
    mode,
    poolConfig,
    60*time.Second,  // Command timeout
    120*time.Second, // SQL timeout
    45*time.Second,  // Function timeout
)
```

#### Métodos Getter/Setter

Puedes obtener y cambiar cada timeout independientemente:

```go
// Obtener timeouts actuales
commandTimeout := handler.GetCommandTimeout()
sqlTimeout := handler.GetSQLTimeout()
functionTimeout := handler.GetFunctionTimeout()

// Cambiar timeouts específicos
handler.SetCommandTimeout(90 * time.Second)
handler.SetSQLTimeout(5 * time.Minute)
handler.SetFunctionTimeout(2 * time.Minute)
```

## ¿Por Qué No Están Relacionados?

**NO**, los timeouts no deberían estar relacionados porque:

1. **Propósitos diferentes**: Cada uno controla un tipo específico de operación
2. **Escalas de tiempo diferentes**: Comandos pueden tardar minutos, heartbeats deben ser rápidos
3. **Impactos diferentes**: Un comando lento no debería desconectar al cliente
4. **Frecuencias diferentes**: Comandos son esporádicos, heartbeats son continuos

## Ejemplo Práctico

```bash
# Comando que tarda 45 segundos
COMMAND:sleep 45

# Con CommandTimeout = 30s
# → El comando se cancela después de 30s
# → El cliente sigue conectado

# Con SQLTimeout = 60s
# → Las consultas SQL pueden tardar hasta 60s
# → Las consultas complejas se cancelan después de 60s

# Con FunctionTimeout = 45s
# → Las funciones pueden tardar hasta 45s
# → Las funciones largas se cancelan después de 45s

# Con HeartbeatTimeout = 5s  
# → El cliente envía PING cada 2s
# → El servidor responde PONG en <5s
# → La conexión se mantiene activa
```

## Beneficios de la Mejora

1. **Configurabilidad**: Puedes ajustar cada timeout según tus necesidades
2. **Flexibilidad**: Diferentes entornos pueden usar diferentes timeouts
3. **Consistencia**: Sigue el mismo patrón que otras configuraciones
4. **Mantenibilidad**: Cambios de timeout no requieren modificar código
5. **Observabilidad**: Puedes ver y cambiar cada timeout independientemente
6. **Granularidad**: Control fino sobre diferentes tipos de operaciones

## Ejemplos de Uso por Entorno

### Para Desarrollo
```bash
./server -command-timeout=60s -sql-timeout=30s -function-timeout=30s
```

### Para Producción
```bash
./server -command-timeout=30s -sql-timeout=120s -function-timeout=60s
```

### Para Análisis de Datos
```bash
./server -command-timeout=60s -sql-timeout=10m -function-timeout=5m
```

### Para Operaciones Críticas
```bash
./server -command-timeout=15s -sql-timeout=30s -function-timeout=30s
```

## Consideraciones de Seguridad

- **Timeouts muy largos**: Pueden permitir que operaciones maliciosas consuman recursos
- **Timeouts muy cortos**: Pueden interrumpir operaciones legítimas
- **Valores recomendados**:
  - CommandTimeout: 30-60 segundos
  - SQLTimeout: 30-300 segundos (dependiendo de la complejidad)
  - FunctionTimeout: 30-120 segundos
  - HeartbeatTimeout: 2-10 segundos
- **Monitoreo**: Usa logs para identificar operaciones que se acercan al timeout 