package server

import (
	"context"
	"log"
)

// ServerFactory provides a convenient way to create and configure a complete server
type ServerFactory struct {
	config *ServerConfig
}

// NewServerFactory creates a new server factory with the given configuration
func NewServerFactory(config *ServerConfig) *ServerFactory {
	return &ServerFactory{
		config: config,
	}
}

// CreateServer creates a fully configured server with all components
func (sf *ServerFactory) CreateServer() (*Handler, *MonitoringManager, error) {
	// Create handler with advanced configuration
	handler := NewHandler(
		sf.config.DeviceID,
		sf.config.AMQPURL,
		sf.config.MySQLDSN,
		"open",
		sf.config.ToPoolConfig(),
		sf.config.CommandTimeout,
		sf.config.SQLTimeout,
		sf.config.FunctionTimeout,
		sf.config, // Pass the full configuration
	)

	// Configure query cache
	handler.SetCacheConfig(sf.config.ToQueryCacheConfig())

	// Configure SQL validation
	handler.SetSQLValidationConfig(sf.config.ToSQLValidationConfig())

	// Configure worker pool
	handler.SetWorkerPoolConfig(sf.config.ToWorkerPoolConfig())

	// Configure rate limiter
	handler.SetRateLimiterConfig(sf.config.ToRateLimiterConfig())

	// Configure heartbeat manager with custom configuration
	heartbeatConfig := sf.config.ToHeartbeatConfig()
	handler.heartbeatManager = NewServerHeartbeatManager(sf.config.DeviceID, heartbeatConfig)

	// Create monitoring manager
	monitoringManager := NewMonitoringManager(handler, sf.config)

	// Register comprehensive monitoring functions
	monitoringManager.RegisterMonitoringFunctions()

	return handler, monitoringManager, nil
}

// StartServer creates and starts a complete server
func (sf *ServerFactory) StartServer(ctx context.Context) error {
	// Create server components
	handler, monitoringManager, err := sf.CreateServer()
	if err != nil {
		return err
	}

	// Display configuration
	monitoringManager.DisplayConfiguration()

	// Start monitoring
	monitoringManager.Start()

	// Start server
	log.Printf("🚀 Starting Full-Featured Enterprise Server...")
	return handler.Start(ctx)
}

// CreateAndConfigureServer is a convenience function that creates a server with default configuration
func CreateAndConfigureServer() (*Handler, *MonitoringManager, error) {
	config := LoadConfigFromFlags()
	factory := NewServerFactory(config)
	return factory.CreateServer()
}

// StartServerWithDefaults is a convenience function that starts a server with default configuration
func StartServerWithDefaults(ctx context.Context) error {
	config := LoadConfigFromFlags()
	factory := NewServerFactory(config)
	return factory.StartServer(ctx)
}
