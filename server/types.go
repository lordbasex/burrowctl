// Package server provides type definitions for the RabbitMQ-based bridge system.
// This file contains all struct definitions and types used throughout the server package.
package server

import (
	"database/sql"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PoolConfig defines database connection pool configuration parameters.
// These settings control how the server manages database connections for optimal performance.
type PoolConfig struct {
	MaxIdleConns    int           // Maximum number of idle connections in the pool
	MaxOpenConns    int           // Maximum number of open connections to the database
	ConnMaxLifetime time.Duration // Maximum amount of time a connection may be reused
}

// Handler is the main server component that manages RabbitMQ connections,
// database operations, and function execution.
//
// The Handler follows a clean architecture pattern where:
// - Core server functionality is separated from business logic
// - Functions are registered dynamically rather than hardcoded
// - Connection management is configurable (pooled vs per-query)
// - Concurrent message processing through worker pools
// - Heartbeat management for connection monitoring
// - Separate queues for RPC and heartbeat operations
type Handler struct {
	config             *ServerConfig          // Configuration for the server
	deviceID           string                 // Unique identifier for this device/server instance
	amqpURL            string                 // RabbitMQ connection URL (amqp://user:pass@host:port/)
	mysqlDSN           string                 // MySQL Data Source Name for database connections
	conn               *amqp.Connection       // Active RabbitMQ connection
	db                 *sql.DB                // Database connection (used in 'open' mode)
	mode               string                 // Connection mode: 'open' (pooled) or 'close' (per-query)
	poolConf           PoolConfig             // Database connection pool configuration
	functionRegistry   map[string]interface{} // Registry of custom functions available for execution
	workerPool         *WorkerPool            // Worker pool for concurrent message processing
	rateLimiter        *RateLimiter           // Rate limiter for controlling request frequency per client
	transactionManager *TransactionManager    // Transaction manager for handling database transactions
	queryCache         *QueryCache            // Query cache for improving performance of repeated queries
	sqlValidator       *SQLValidator          // SQL validator for security and policy enforcement

	// Heartbeat management
	heartbeatManager *ServerHeartbeatManager // Heartbeat manager for connection monitoring

	// Command execution configuration
	commandTimeout time.Duration // Timeout for command execution

	// Query execution configuration
	sqlTimeout      time.Duration // Timeout for SQL queries
	functionTimeout time.Duration // Timeout for function calls

	// Queue management
	rpcQueueName       string // RPC queue name for this device
	heartbeatQueueName string // Heartbeat queue name for this device
}

// FunctionParam represents a single parameter for function execution.
// It includes type information for proper parameter conversion using reflection.
type FunctionParam struct {
	Type  string      `json:"type"`  // Parameter type (e.g., "string", "int", "float64", "bool")
	Value interface{} `json:"value"` // Actual parameter value (will be converted to the specified type)
}

// FunctionRequest represents a complete function call request.
// It contains the function name and all parameters required for execution.
type FunctionRequest struct {
	Name   string          `json:"name"`   // Name of the function to execute
	Params []FunctionParam `json:"params"` // Array of parameters with type information
}

// RPCRequest represents an incoming request from a client.
// It contains all necessary information to process SQL queries, function calls, or system commands.
type RPCRequest struct {
	Type          string        `json:"type"`          // Request type: "sql", "function", "command", or "transaction"
	DeviceID      string        `json:"deviceID"`      // Target device ID for request routing
	Query         string        `json:"query"`         // SQL query, function JSON, or system command
	Params        []interface{} `json:"params"`        // Parameters for SQL queries (empty for functions/commands)
	ClientIP      string        `json:"clientIP"`      // Client IP address for logging and security
	TransactionID string        `json:"transactionID"` // Transaction ID for transaction-aware operations
	Command       string        `json:"command"`       // Transaction command (BEGIN, COMMIT, ROLLBACK)
}

// RPCResponse represents the response sent back to clients.
// It follows a consistent format regardless of the request type.
type RPCResponse struct {
	Columns []string        `json:"columns"` // Column names for tabular data
	Rows    [][]interface{} `json:"rows"`    // Data rows (each row is an array of values)
	Error   string          `json:"error"`   // Error message if operation failed (empty on success)
}
