package server

import (
	"flag"
	"os"
	"strconv"
	"time"

	"github.com/lordbasex/burrowctl/client"
)

// ServerConfig holds all configuration options for the server
type ServerConfig struct {
	// Device and connection configuration
	DeviceID string
	AMQPURL  string
	MySQLDSN string

	// Cache configuration
	CacheEnabled bool
	CacheSize    int
	CacheTTL     time.Duration
	CacheCleanup time.Duration

	// SQL Validation configuration
	ValidationEnabled bool
	StrictMode        bool
	AllowDDL          bool
	AllowDML          bool
	AllowStoredProcs  bool
	MaxQueryLength    int
	LogViolations     bool

	// Performance configuration
	Workers   int
	QueueSize int
	RateLimit int
	BurstSize int

	// Database configuration
	PoolIdle     int
	PoolOpen     int
	ConnLifetime time.Duration

	// Monitoring configuration
	MonitoringEnabled  bool
	MonitoringInterval time.Duration

	// Heartbeat configuration
	HeartbeatEnabled      bool
	HeartbeatInterval     time.Duration
	HeartbeatTimeout      time.Duration
	HeartbeatMaxMissed    int
	HeartbeatCleanup      time.Duration
	HeartbeatMaxClientAge time.Duration

	// Command execution configuration
	CommandTimeout time.Duration

	// Query execution configuration
	SQLTimeout      time.Duration
	FunctionTimeout time.Duration

	// Reconnection configuration
	ReconnectEnabled           bool
	ReconnectMaxAttempts       int
	ReconnectInitialInterval   time.Duration
	ReconnectMaxInterval       time.Duration
	ReconnectBackoffMultiplier float64
	ReconnectResetInterval     time.Duration
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		// Device and connection configuration
		DeviceID: "my-device",
		AMQPURL:  "amqp://burrowuser:burrowpass123@localhost:5672/",
		MySQLDSN: "burrowuser:burrowpass123@tcp(localhost:3306)/burrowdb",

		// Cache configuration
		CacheEnabled: true,
		CacheSize:    2000,
		CacheTTL:     15 * time.Minute,
		CacheCleanup: 5 * time.Minute,

		// SQL Validation configuration
		ValidationEnabled: true,
		StrictMode:        false,
		AllowDDL:          false,
		AllowDML:          true,
		AllowStoredProcs:  false,
		MaxQueryLength:    10000,
		LogViolations:     true,

		// Performance configuration
		Workers:   25,
		QueueSize: 1000,
		RateLimit: 100,
		BurstSize: 200,

		// Database configuration
		PoolIdle:     25,
		PoolOpen:     75,
		ConnLifetime: 10 * time.Minute,

		// Monitoring configuration
		MonitoringEnabled:  true,
		MonitoringInterval: 60 * time.Second,

		// Heartbeat configuration
		HeartbeatEnabled:      true,
		HeartbeatInterval:     30 * time.Second,
		HeartbeatTimeout:      5 * time.Second,
		HeartbeatMaxMissed:    3,
		HeartbeatCleanup:      1 * time.Minute,
		HeartbeatMaxClientAge: 2 * time.Minute,

		// Command execution configuration
		CommandTimeout: 30 * time.Second,

		// Query execution configuration
		SQLTimeout:      30 * time.Second,
		FunctionTimeout: 30 * time.Second,

		// Reconnection configuration
		ReconnectEnabled:           true,
		ReconnectMaxAttempts:       5,
		ReconnectInitialInterval:   5 * time.Second,
		ReconnectMaxInterval:       30 * time.Second,
		ReconnectBackoffMultiplier: 2.0,
		ReconnectResetInterval:     1 * time.Hour,
	}
}

// LoadConfigFromFlags loads configuration from command line flags
func LoadConfigFromFlags() *ServerConfig {
	config := DefaultServerConfig()

	// Cache configuration flags
	flag.BoolVar(&config.CacheEnabled, "cache-enabled", config.CacheEnabled, "Enable query caching")
	flag.IntVar(&config.CacheSize, "cache-size", config.CacheSize, "Maximum number of cached queries")
	flag.DurationVar(&config.CacheTTL, "cache-ttl", config.CacheTTL, "Cache TTL duration")
	flag.DurationVar(&config.CacheCleanup, "cache-cleanup", config.CacheCleanup, "Cache cleanup interval")

	// SQL Validation configuration flags
	flag.BoolVar(&config.ValidationEnabled, "validation-enabled", config.ValidationEnabled, "Enable SQL validation")
	flag.BoolVar(&config.StrictMode, "strict-mode", config.StrictMode, "Enable strict validation mode")
	flag.BoolVar(&config.AllowDDL, "allow-ddl", config.AllowDDL, "Allow Data Definition Language commands")
	flag.BoolVar(&config.AllowDML, "allow-dml", config.AllowDML, "Allow Data Manipulation Language commands")
	flag.BoolVar(&config.AllowStoredProcs, "allow-stored-procs", config.AllowStoredProcs, "Allow stored procedure calls")
	flag.IntVar(&config.MaxQueryLength, "max-query-length", config.MaxQueryLength, "Maximum query length in characters")
	flag.BoolVar(&config.LogViolations, "log-violations", config.LogViolations, "Log validation violations")

	// Performance configuration flags
	flag.IntVar(&config.Workers, "workers", config.Workers, "Number of worker goroutines")
	flag.IntVar(&config.QueueSize, "queue-size", config.QueueSize, "Worker queue size")
	flag.IntVar(&config.RateLimit, "rate-limit", config.RateLimit, "Rate limit per client IP (requests per second)")
	flag.IntVar(&config.BurstSize, "burst-size", config.BurstSize, "Rate limit burst size")

	// Database configuration flags
	flag.IntVar(&config.PoolIdle, "pool-idle", config.PoolIdle, "Maximum idle database connections")
	flag.IntVar(&config.PoolOpen, "pool-open", config.PoolOpen, "Maximum open database connections")
	flag.DurationVar(&config.ConnLifetime, "conn-lifetime", config.ConnLifetime, "Database connection lifetime")

	// Monitoring configuration flags
	flag.BoolVar(&config.MonitoringEnabled, "monitoring-enabled", config.MonitoringEnabled, "Enable periodic monitoring")
	flag.DurationVar(&config.MonitoringInterval, "monitoring-interval", config.MonitoringInterval, "Monitoring report interval")

	// Heartbeat configuration flags
	flag.BoolVar(&config.HeartbeatEnabled, "heartbeat-enabled", config.HeartbeatEnabled, "Enable server heartbeat")
	flag.DurationVar(&config.HeartbeatInterval, "heartbeat-interval", config.HeartbeatInterval, "Server heartbeat interval")
	flag.DurationVar(&config.HeartbeatTimeout, "heartbeat-timeout", config.HeartbeatTimeout, "Server heartbeat timeout")
	flag.IntVar(&config.HeartbeatMaxMissed, "heartbeat-max-missed", config.HeartbeatMaxMissed, "Maximum missed heartbeats before disconnect")
	flag.DurationVar(&config.HeartbeatCleanup, "heartbeat-cleanup", config.HeartbeatCleanup, "Heartbeat cleanup interval")
	flag.DurationVar(&config.HeartbeatMaxClientAge, "heartbeat-max-client-age", config.HeartbeatMaxClientAge, "Maximum age for client heartbeat records")

	// Command execution configuration flags
	flag.DurationVar(&config.CommandTimeout, "command-timeout", config.CommandTimeout, "Timeout for command execution")
	flag.DurationVar(&config.SQLTimeout, "sql-timeout", config.SQLTimeout, "Timeout for SQL queries")
	flag.DurationVar(&config.FunctionTimeout, "function-timeout", config.FunctionTimeout, "Timeout for function calls")

	// Reconnection configuration flags
	flag.BoolVar(&config.ReconnectEnabled, "reconnect-enabled", config.ReconnectEnabled, "Enable client reconnection logic")
	flag.IntVar(&config.ReconnectMaxAttempts, "reconnect-max-attempts", config.ReconnectMaxAttempts, "Maximum reconnection attempts")
	flag.DurationVar(&config.ReconnectInitialInterval, "reconnect-initial-interval", config.ReconnectInitialInterval, "Initial interval for reconnection attempts")
	flag.DurationVar(&config.ReconnectMaxInterval, "reconnect-max-interval", config.ReconnectMaxInterval, "Maximum interval for reconnection attempts")
	flag.Float64Var(&config.ReconnectBackoffMultiplier, "reconnect-backoff-multiplier", config.ReconnectBackoffMultiplier, "Multiplier for exponential backoff")
	flag.DurationVar(&config.ReconnectResetInterval, "reconnect-reset-interval", config.ReconnectResetInterval, "Interval to reset backoff multiplier")

	flag.Parse()

	// Load environment variables (override flags)
	config.DeviceID = getEnv("DEVICE_ID", config.DeviceID)
	config.AMQPURL = getEnv("AMQP_URL", config.AMQPURL)
	config.MySQLDSN = getEnv("MYSQL_DSN", config.MySQLDSN)

	// Load heartbeat configuration from environment variables
	config.HeartbeatEnabled = getEnvBool("HEARTBEAT_ENABLED", config.HeartbeatEnabled)
	config.HeartbeatInterval = getEnvDuration("HEARTBEAT_INTERVAL", config.HeartbeatInterval)
	config.HeartbeatTimeout = getEnvDuration("HEARTBEAT_TIMEOUT", config.HeartbeatTimeout)
	config.HeartbeatMaxMissed = getEnvInt("HEARTBEAT_MAX_MISSED", config.HeartbeatMaxMissed)
	config.HeartbeatCleanup = getEnvDuration("HEARTBEAT_CLEANUP", config.HeartbeatCleanup)
	config.HeartbeatMaxClientAge = getEnvDuration("HEARTBEAT_MAX_CLIENT_AGE", config.HeartbeatMaxClientAge)

	// Load command execution configuration from environment variables
	config.CommandTimeout = getEnvDuration("COMMAND_TIMEOUT", config.CommandTimeout)
	config.SQLTimeout = getEnvDuration("SQL_TIMEOUT", config.SQLTimeout)
	config.FunctionTimeout = getEnvDuration("FUNCTION_TIMEOUT", config.FunctionTimeout)

	// Load reconnection configuration from environment variables
	config.ReconnectEnabled = getEnvBool("RECONNECT_ENABLED", config.ReconnectEnabled)
	config.ReconnectMaxAttempts = getEnvInt("RECONNECT_MAX_ATTEMPTS", config.ReconnectMaxAttempts)
	config.ReconnectInitialInterval = getEnvDuration("RECONNECT_INITIAL_INTERVAL", config.ReconnectInitialInterval)
	config.ReconnectMaxInterval = getEnvDuration("RECONNECT_MAX_INTERVAL", config.ReconnectMaxInterval)
	config.ReconnectBackoffMultiplier = getEnvFloat64("RECONNECT_BACKOFF_MULTIPLIER", config.ReconnectBackoffMultiplier)
	config.ReconnectResetInterval = getEnvDuration("RECONNECT_RESET_INTERVAL", config.ReconnectResetInterval)

	return config
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool gets an environment variable as a boolean or returns a default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

// getEnvInt gets an environment variable as an integer or returns a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

// getEnvDuration gets an environment variable as a duration or returns a default value
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

// getEnvFloat64 gets an environment variable as a float64 or returns a default value
func getEnvFloat64(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return defaultValue
}

// ToPoolConfig converts ServerConfig to PoolConfig
func (sc *ServerConfig) ToPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxIdleConns:    sc.PoolIdle,
		MaxOpenConns:    sc.PoolOpen,
		ConnMaxLifetime: sc.ConnLifetime,
	}
}

// ToQueryCacheConfig converts ServerConfig to QueryCacheConfig
func (sc *ServerConfig) ToQueryCacheConfig() QueryCacheConfig {
	return QueryCacheConfig{
		MaxSize:         sc.CacheSize,
		TTL:             sc.CacheTTL,
		CleanupInterval: sc.CacheCleanup,
		Enabled:         sc.CacheEnabled,
	}
}

// ToSQLValidationConfig converts ServerConfig to SQLValidationConfig
func (sc *ServerConfig) ToSQLValidationConfig() SQLValidationConfig {
	return SQLValidationConfig{
		Enabled:               sc.ValidationEnabled,
		AllowedCommands:       sc.buildAllowedCommands(),
		BlockedCommands:       sc.buildBlockedCommands(),
		AllowDDL:              sc.AllowDDL,
		AllowDML:              sc.AllowDML,
		AllowDQL:              true, // Always allow SELECT queries
		AllowStoredProcedures: sc.AllowStoredProcs,
		MaxQueryLength:        sc.MaxQueryLength,
		StrictMode:            sc.StrictMode,
		LogViolations:         sc.LogViolations,
	}
}

// ToWorkerPoolConfig converts ServerConfig to WorkerPoolConfig
func (sc *ServerConfig) ToWorkerPoolConfig() *WorkerPoolConfig {
	return &WorkerPoolConfig{
		WorkerCount: sc.Workers,
		QueueSize:   sc.QueueSize,
		Timeout:     30 * time.Second,
	}
}

// ToRateLimiterConfig converts ServerConfig to RateLimiterConfig
func (sc *ServerConfig) ToRateLimiterConfig() *RateLimiterConfig {
	return &RateLimiterConfig{
		RequestsPerSecond: sc.RateLimit,
		BurstSize:         sc.BurstSize,
		CleanupInterval:   5 * time.Minute,
	}
}

// buildAllowedCommands constructs the list of allowed SQL commands based on configuration
func (sc *ServerConfig) buildAllowedCommands() []string {
	var commands []string

	// Always allow basic query commands
	commands = append(commands, "SELECT", "SHOW", "DESCRIBE", "EXPLAIN")

	// Add DML commands if allowed
	if sc.AllowDML {
		commands = append(commands, "INSERT", "UPDATE", "DELETE")
	}

	// Add DDL commands if allowed
	if sc.AllowDDL {
		commands = append(commands, "CREATE", "ALTER", "DROP", "TRUNCATE")
	}

	// Add stored procedure commands if allowed
	if sc.AllowStoredProcs {
		commands = append(commands, "CALL", "EXEC", "EXECUTE")
	}

	return commands
}

// buildBlockedCommands constructs the list of explicitly blocked commands
func (sc *ServerConfig) buildBlockedCommands() []string {
	blocked := []string{
		"SHUTDOWN", "RESTART", "RESET",
		"CREATE USER", "DROP USER", "ALTER USER",
		"GRANT", "REVOKE", "FLUSH",
		"LOAD DATA", "LOAD XML",
		"INTO OUTFILE", "INTO DUMPFILE",
	}

	// In strict mode, add more restricted commands
	if sc.StrictMode {
		blocked = append(blocked,
			"TRUNCATE", "DELETE", // Restrict bulk operations
			"ALTER", "CREATE", // Restrict schema changes
			"CALL", "EXECUTE", // Restrict stored procedures
		)
	}

	return blocked
}

// ToHeartbeatConfig converts ServerConfig to ServerHeartbeatConfig
func (sc *ServerConfig) ToHeartbeatConfig() *ServerHeartbeatConfig {
	return &ServerHeartbeatConfig{
		Enabled:         sc.HeartbeatEnabled,
		ResponseTimeout: sc.HeartbeatTimeout,
		CleanupInterval: sc.HeartbeatCleanup,
		MaxClientAge:    sc.HeartbeatMaxClientAge,
	}
}

// ToReconnectConfig converts ServerConfig to ReconnectConfig
func (sc *ServerConfig) ToReconnectConfig() *client.ReconnectConfig {
	return &client.ReconnectConfig{
		Enabled:           sc.ReconnectEnabled,
		MaxAttempts:       sc.ReconnectMaxAttempts,
		InitialInterval:   sc.ReconnectInitialInterval,
		MaxInterval:       sc.ReconnectMaxInterval,
		BackoffMultiplier: sc.ReconnectBackoffMultiplier,
		ResetInterval:     sc.ReconnectResetInterval,
	}
}
