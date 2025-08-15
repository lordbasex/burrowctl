package client

import (
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ReconnectConfig holds configuration for automatic reconnection behavior.
// These settings control how the client handles connection failures and recovery.
type ReconnectConfig struct {
	Enabled           bool          // Whether automatic reconnection is enabled
	MaxAttempts       int           // Maximum number of reconnection attempts (0 = unlimited)
	InitialInterval   time.Duration // Initial wait time between reconnection attempts
	MaxInterval       time.Duration // Maximum wait time between reconnection attempts
	BackoffMultiplier float64       // Multiplier for exponential backoff (e.g., 2.0)
	ResetInterval     time.Duration // Time after which to reset backoff to initial interval
}

// DefaultReconnectConfig returns a sensible default reconnection configuration.
func DefaultReconnectConfig() *ReconnectConfig {
	return &ReconnectConfig{
		Enabled:           true,
		MaxAttempts:       10,               // Try up to 10 times
		InitialInterval:   1 * time.Second,  // Start with 1 second
		MaxInterval:       60 * time.Second, // Cap at 60 seconds
		BackoffMultiplier: 2.0,              // Double each time
		ResetInterval:     5 * time.Minute,  // Reset after 5 minutes of success
	}
}

// ConnectionManager handles automatic reconnection for RabbitMQ connections.
// It provides transparent reconnection with exponential backoff and connection health monitoring.
type ConnectionManager struct {
	config     *ReconnectConfig // Reconnection configuration
	dsn        string           // Original DSN for reconnection
	conn       *amqp.Connection // Current connection (nil if disconnected)
	connConfig *DSNConfig       // Parsed DSN configuration

	// State management
	mutex         sync.RWMutex  // Protects connection state
	isConnected   bool          // Current connection status
	lastConnected time.Time     // Last successful connection time
	attempts      int           // Current number of reconnection attempts
	nextInterval  time.Duration // Next reconnection interval
	lastError     error         // Last connection error

	// Callbacks
	onConnected    func()      // Called when connection is established
	onDisconnected func(error) // Called when connection is lost
}

// NewConnectionManager creates a new connection manager with the specified configuration.
//
// Parameters:
//   - dsn: Data Source Name for the connection
//   - config: Reconnection configuration (nil for defaults)
//
// Returns:
//   - *ConnectionManager: Configured connection manager
//   - error: Any error that occurred during initialization
func NewConnectionManager(dsn string, config *ReconnectConfig) (*ConnectionManager, error) {
	if config == nil {
		config = DefaultReconnectConfig()
	}

	// Parse DSN to validate it
	connConfig, err := parseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}

	cm := &ConnectionManager{
		config:       config,
		dsn:          dsn,
		connConfig:   connConfig,
		nextInterval: config.InitialInterval,
	}

	return cm, nil
}

// Connect establishes the initial connection with automatic reconnection if enabled.
//
// Returns:
//   - error: Any error that occurred during connection establishment
func (cm *ConnectionManager) Connect() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	return cm.doConnect()
}

// doConnect performs the actual connection (must be called with mutex held).
func (cm *ConnectionManager) doConnect() error {

	var err error

	if strings.HasPrefix(cm.connConfig.AMQPURL, "amqps://") {

		// cortar url de amqpURL luego de la @ y el :
		domain := strings.Split(cm.connConfig.AMQPURL, "@")[1]
		domain = strings.Split(domain, ":")[0]

		// TLS explícito con SNI (opcional pero recomendable)
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: domain,
		}

		cm.conn, err = amqp.DialTLS(cm.connConfig.AMQPURL, tlsCfg)
	} else {
		cm.conn, err = amqp.Dial(cm.connConfig.AMQPURL)
	}

	if err != nil {
		cm.lastError = err
		if cm.config.Enabled {
			cm.logf("Connection failed, will retry: %v", err)
		}
		return err
	}

	cm.isConnected = true
	cm.lastConnected = time.Now()
	cm.attempts = 0
	cm.nextInterval = cm.config.InitialInterval
	cm.lastError = nil

	// Set up connection close notification
	if cm.config.Enabled {
		go cm.monitorConnection()
	}

	// Call connection callback
	if cm.onConnected != nil {
		go cm.onConnected()
	}

	cm.logf("Connected to RabbitMQ %s", cm.connConfig.AMQPURL)
	return nil
}

// monitorConnection monitors the connection for closure and triggers reconnection.
func (cm *ConnectionManager) monitorConnection() {
	if cm.conn == nil {
		return
	}

	// Wait for connection to close
	closeErr := <-cm.conn.NotifyClose(make(chan *amqp.Error))

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.isConnected {
		// Connection was closed intentionally
		return
	}

	cm.isConnected = false
	cm.conn = nil

	var err error
	if closeErr != nil {
		err = fmt.Errorf("connection lost: %v", closeErr)
	} else {
		err = fmt.Errorf("connection closed unexpectedly")
	}

	cm.lastError = err
	cm.logf("Connection lost: %v", err)

	// Call disconnection callback
	if cm.onDisconnected != nil {
		go cm.onDisconnected(err)
	}

	// Start reconnection process
	if cm.config.Enabled {
		go cm.reconnectLoop()
	}
}

// reconnectLoop handles the reconnection process with exponential backoff.
func (cm *ConnectionManager) reconnectLoop() {
	for {
		// Check if we should stop trying
		if cm.config.MaxAttempts > 0 && cm.attempts >= cm.config.MaxAttempts {
			cm.logf("Maximum reconnection attempts (%d) reached, giving up", cm.config.MaxAttempts)
			return
		}

		// Wait before attempting reconnection
		time.Sleep(cm.nextInterval)

		cm.mutex.Lock()

		// Check if connection was restored by another goroutine
		if cm.isConnected {
			cm.mutex.Unlock()
			return
		}

		cm.attempts++
		cm.logf("Reconnection attempt %d/%d", cm.attempts, cm.config.MaxAttempts)

		err := cm.doConnect()
		if err == nil {
			cm.mutex.Unlock()
			cm.logf("Reconnection successful after %d attempts", cm.attempts)
			return
		}

		// Calculate next backoff interval
		cm.nextInterval = time.Duration(float64(cm.nextInterval) * cm.config.BackoffMultiplier)
		if cm.nextInterval > cm.config.MaxInterval {
			cm.nextInterval = cm.config.MaxInterval
		}

		cm.mutex.Unlock()
		cm.logf("Reconnection attempt %d failed: %v, next attempt in %v", cm.attempts, err, cm.nextInterval)
	}
}

// GetConnection returns the current connection if available.
// This method is thread-safe and returns nil if not connected.
//
// Returns:
//   - *amqp.Connection: Current connection or nil if disconnected
//   - error: Current connection error if any
func (cm *ConnectionManager) GetConnection() (*amqp.Connection, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if cm.isConnected && cm.conn != nil {
		return cm.conn, nil
	}

	if cm.lastError != nil {
		return nil, fmt.Errorf("not connected: %w", cm.lastError)
	}

	return nil, fmt.Errorf("not connected")
}

// IsConnected returns whether the connection is currently established.
//
// Returns:
//   - bool: true if connected, false otherwise
func (cm *ConnectionManager) IsConnected() bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.isConnected
}

// Close closes the connection and disables automatic reconnection.
//
// Returns:
//   - error: Any error that occurred during closure
func (cm *ConnectionManager) Close() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.isConnected = false // Prevent reconnection

	if cm.conn != nil {
		err := cm.conn.Close()
		cm.conn = nil
		return err
	}

	return nil
}

// SetCallbacks sets callback functions for connection events.
//
// Parameters:
//   - onConnected: Called when connection is established (can be nil)
//   - onDisconnected: Called when connection is lost (can be nil)
func (cm *ConnectionManager) SetCallbacks(onConnected func(), onDisconnected func(error)) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.onConnected = onConnected
	cm.onDisconnected = onDisconnected
}

// GetStats returns current connection statistics.
//
// Returns:
//   - ConnectionStats: Current connection statistics
func (cm *ConnectionManager) GetStats() ConnectionStats {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	var uptime time.Duration
	if cm.isConnected {
		uptime = time.Since(cm.lastConnected)
	}

	return ConnectionStats{
		IsConnected:     cm.isConnected,
		LastConnected:   cm.lastConnected,
		Uptime:          uptime,
		ReconnectCount:  cm.attempts,
		LastError:       cm.lastError,
		NextReconnectIn: cm.nextInterval,
	}
}

// Reconnect manually triggers a reconnection attempt.
// This is useful when the heartbeat manager detects connection issues.
//
// Returns:
//   - error: Any error that occurred during reconnection
func (cm *ConnectionManager) Reconnect() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Reset connection state
	cm.isConnected = false
	if cm.conn != nil {
		cm.conn.Close()
		cm.conn = nil
	}

	// Attempt to reconnect
	return cm.doConnect()
}

// ConnectionStats contains statistics about the connection state.
type ConnectionStats struct {
	IsConnected     bool          // Whether currently connected
	LastConnected   time.Time     // Time of last successful connection
	Uptime          time.Duration // How long the current connection has been up
	ReconnectCount  int           // Number of reconnection attempts
	LastError       error         // Last connection error
	NextReconnectIn time.Duration // Time until next reconnection attempt
}

// logf provides conditional debug logging for the connection manager.
func (cm *ConnectionManager) logf(format string, args ...interface{}) {
	if cm.connConfig != nil && cm.connConfig.Debug {
		log.Printf("[reconnect] "+format, args...)
	}
}
