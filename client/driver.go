// Package client provides a database/sql driver implementation for the burrowctl system.
// This package enables standard SQL database operations through RabbitMQ message queues,
// allowing remote database access through devices behind NAT or firewalls.
//
// The client follows Go's database/sql driver interface, making it compatible with
// standard SQL operations while routing them through RabbitMQ to remote servers.
//
// Key features:
// - Standard database/sql driver interface compliance
// - RabbitMQ-based transport layer
// - Configurable timeouts and debugging
// - Support for SQL queries, function calls, and system commands
// - Automatic connection management and error handling
package client

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Package initialization registers the driver with the database/sql package.
// This allows users to use sql.Open("rabbitsql", dsn) to create connections.
func init() {
	sql.Register("rabbitsql", &Driver{})
}

// Driver implements the database/sql/driver.Driver interface.
// It provides the entry point for creating new database connections
// through the RabbitMQ transport layer.
type Driver struct{}

// Open creates a new database connection using the provided Data Source Name (DSN).
// The DSN must contain RabbitMQ connection details and device identification.
//
// DSN Format:
//
//	deviceID=<device-id>&amqp_uri=<rabbitmq-url>&timeout=<timeout>&debug=<boolean>&reconnect_enabled=<boolean>&reconnect_max_attempts=<int>&reconnect_initial_interval=<duration>&reconnect_max_interval=<duration>&reconnect_backoff_multiplier=<float>&reconnect_reset_interval=<duration>
//
// Parameters:
//   - deviceID: Unique identifier for the target device/server
//   - amqp_uri: RabbitMQ connection URL (e.g., "amqp://user:pass@localhost:5672/" or "amqps://user:pass@localhost:5671/")
//   - timeout: Query timeout duration (optional, default: 5s)
//   - debug: Enable debug logging (optional, default: false)
//   - reconnect_enabled: Enable automatic reconnection (optional, default: true)
//   - reconnect_max_attempts: Maximum reconnection attempts (optional, default: 10)
//   - reconnect_initial_interval: Initial interval between attempts (optional, default: 1s)
//   - reconnect_max_interval: Maximum interval between attempts (optional, default: 60s)
//   - reconnect_backoff_multiplier: Backoff multiplier (optional, default: 2.0)
//   - reconnect_reset_interval: Reset interval for backoff (optional, default: 5m)
//
// Returns:
//   - driver.Conn: A connection instance ready for SQL operations
//   - error: Any error that occurred during connection establishment
//
// Example:
//
//	dsn := "deviceID=my-device&amqp_uri=amqp://user:pass@localhost:5672/&timeout=10s&debug=true&reconnect_max_attempts=20&reconnect_initial_interval=2s"
//	db, err := sql.Open("rabbitsql", dsn)
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	// Parse and validate the DSN configuration
	conf, err := parseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("DSN parsing failed: %v", err)
	}

	// Create connection manager with automatic reconnection
	reconnectConfig := &ReconnectConfig{
		Enabled:           conf.ReconnectEnabled,
		MaxAttempts:       conf.ReconnectMaxAttempts,
		InitialInterval:   conf.ReconnectInitialInterval,
		MaxInterval:       conf.ReconnectMaxInterval,
		BackoffMultiplier: conf.ReconnectBackoffMultiplier,
		ResetInterval:     conf.ReconnectResetInterval,
	}

	connMgr, err := NewConnectionManager(dsn, reconnectConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	// Establish initial connection
	if err := connMgr.Connect(); err != nil {
		return nil, fmt.Errorf("RabbitMQ connection failed to '%s': %v\nPlease check:\n- RabbitMQ server is running\n- Credentials are correct\n- Network connectivity", conf.AMQPURL, err)
	}

	// Log successful connection if debug mode is enabled
	if conf.Debug {
		log.Printf("[client debug] Connected to RabbitMQ %s (deviceID=%s, timeout=%v)", conf.AMQPURL, conf.DeviceID, conf.Timeout)
	}

	// Return a new connection instance
	conn := &Conn{
		deviceID: conf.DeviceID,
		connMgr:  connMgr,
		config:   conf,
	}

	// Setup heartbeat manager if enabled
	conn.setupHeartbeat()

	return conn, nil
}

// DSNConfig holds the parsed configuration from a Data Source Name.
// It contains all necessary parameters for establishing and managing
// the RabbitMQ connection and client behavior.
type DSNConfig struct {
	DeviceID string        // Unique identifier for the target device/server
	AMQPURL  string        // RabbitMQ connection URL with credentials
	Timeout  time.Duration // Maximum time to wait for query responses
	Debug    bool          // Whether to enable debug logging

	// Heartbeat configuration
	HeartbeatEnabled bool             // Whether heartbeat is enabled
	HeartbeatConfig  *HeartbeatConfig // Heartbeat configuration

	// Reconnection configuration
	ReconnectEnabled           bool          // Whether reconnection is enabled
	ReconnectMaxAttempts       int           // Maximum reconnection attempts
	ReconnectInitialInterval   time.Duration // Initial interval between attempts
	ReconnectMaxInterval       time.Duration // Maximum interval between attempts
	ReconnectBackoffMultiplier float64       // Backoff multiplier for exponential backoff
	ReconnectResetInterval     time.Duration // Interval to reset backoff
}

// parseDSN parses a Data Source Name string into a structured configuration.
// It validates required parameters and provides sensible defaults for optional ones.
//
// The DSN format follows URL query parameter conventions:
//
//	key1=value1&key2=value2&key3=value3
//
// Required parameters:
//   - deviceID: Target device identifier
//   - amqp_uri: RabbitMQ connection URL
//
// Optional parameters:
//   - timeout: Query timeout (default: 5s)
//   - debug: Debug logging (default: false)
//
// Parameters:
//   - dsn: The Data Source Name string to parse
//
// Returns:
//   - *DSNConfig: Parsed and validated configuration
//   - error: Any parsing or validation error
func parseDSN(dsn string) (*DSNConfig, error) {
	// Parse DSN as URL query parameters
	u, err := url.Parse("?" + dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN format: %v", err)
	}

	values := u.Query()

	// Validate required parameters
	deviceID := values.Get("deviceID")
	if deviceID == "" {
		return nil, fmt.Errorf("missing required parameter 'deviceID' in DSN")
	}

	amqpURI := values.Get("amqp_uri")
	if amqpURI == "" {
		return nil, fmt.Errorf("missing required parameter 'amqp_uri' in DSN")
	}

	// Validate AMQP URI format amqp:// or amqps://
	if len(amqpURI) < 7 || (amqpURI[:7] != "amqp://" && amqpURI[:8] != "amqps://") {
		return nil, fmt.Errorf("invalid amqp_uri format: must start with 'amqp://' or 'amqps://'")
	}

	// Parse optional timeout parameter
	timeoutStr := values.Get("timeout")
	timeout := 5 * time.Second // Default timeout
	if timeoutStr != "" {
		parsedTimeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout format '%s': %v (example: '5s', '30s', '1m')", timeoutStr, err)
		}
		timeout = parsedTimeout
	}

	// Parse optional debug parameter
	debugStr := strings.ToLower(values.Get("debug"))
	debug := debugStr == "true" || debugStr == "1"

	// Parse reconnection configuration
	reconnectEnabled := true // Default to enabled
	if reconnectStr := strings.ToLower(values.Get("reconnect_enabled")); reconnectStr != "" {
		reconnectEnabled = reconnectStr == "true" || reconnectStr == "1"
	}

	reconnectMaxAttempts := 10 // Default value
	if maxAttemptsStr := values.Get("reconnect_max_attempts"); maxAttemptsStr != "" {
		if maxAttempts, err := strconv.Atoi(maxAttemptsStr); err == nil && maxAttempts >= 0 {
			reconnectMaxAttempts = maxAttempts
		}
	}

	reconnectInitialInterval := 1 * time.Second // Default value
	if initialIntervalStr := values.Get("reconnect_initial_interval"); initialIntervalStr != "" {
		if initialInterval, err := time.ParseDuration(initialIntervalStr); err == nil {
			reconnectInitialInterval = initialInterval
		}
	}

	reconnectMaxInterval := 60 * time.Second // Default value
	if maxIntervalStr := values.Get("reconnect_max_interval"); maxIntervalStr != "" {
		if maxInterval, err := time.ParseDuration(maxIntervalStr); err == nil {
			reconnectMaxInterval = maxInterval
		}
	}

	reconnectBackoffMultiplier := 2.0 // Default value
	if backoffMultiplierStr := values.Get("reconnect_backoff_multiplier"); backoffMultiplierStr != "" {
		if backoffMultiplier, err := strconv.ParseFloat(backoffMultiplierStr, 64); err == nil && backoffMultiplier > 0 {
			reconnectBackoffMultiplier = backoffMultiplier
		}
	}

	reconnectResetInterval := 5 * time.Minute // Default value
	if resetIntervalStr := values.Get("reconnect_reset_interval"); resetIntervalStr != "" {
		if resetInterval, err := time.ParseDuration(resetIntervalStr); err == nil {
			reconnectResetInterval = resetInterval
		}
	}

	// Create and return configuration
	// Create heartbeat configuration with LogLevel from debug setting
	heartbeatConfig := DefaultHeartbeatConfig()
	heartbeatConfig.LogLevel = debug // Use debug setting for heartbeat logging

	conf := &DSNConfig{
		DeviceID:                   deviceID,
		AMQPURL:                    amqpURI,
		Timeout:                    timeout,
		Debug:                      debug,
		HeartbeatEnabled:           true, // Enable heartbeat by default
		HeartbeatConfig:            heartbeatConfig,
		ReconnectEnabled:           reconnectEnabled,
		ReconnectMaxAttempts:       reconnectMaxAttempts,
		ReconnectInitialInterval:   reconnectInitialInterval,
		ReconnectMaxInterval:       reconnectMaxInterval,
		ReconnectBackoffMultiplier: reconnectBackoffMultiplier,
		ReconnectResetInterval:     reconnectResetInterval,
	}

	return conf, nil
}
