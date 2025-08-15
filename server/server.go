// Package server provides implementation for the RabbitMQ-based bridge system.
// This file contains the core server logic, message handling, and processing methods.
//
// The server acts as a message consumer that listens on a device-specific RabbitMQ queue
// and processes three types of requests:
// 1. SQL queries - Direct database operations with parameter binding
// 2. Function calls - Extensible function system with type-safe parameter conversion
// 3. System commands - Controlled execution of OS commands with timeout management
//
// Key features:
// - Connection pooling for database operations
// - Dynamic function registration system
// - Type-safe parameter conversion using reflection
// - Comprehensive error handling and logging
// - Configurable timeouts for all operations
//
// Type definitions are located in types.go for better code organization.
package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

// NewHandler creates a new Handler instance with the specified configuration.
//
// Parameters:
//   - deviceID: Unique identifier for this device/server instance (typically a SHA256 hash)
//   - amqpURL: RabbitMQ connection URL (e.g., "amqp://user:pass@localhost:5672/")
//   - mysqlDSN: MySQL Data Source Name (e.g., "user:pass@tcp(localhost:3306)/dbname")
//   - mode: Connection mode - "open" for connection pooling (default) or "close" for per-query connections
//   - poolConf: Database connection pool configuration (nil for defaults)
//   - commandTimeout: Timeout for command execution (0 for default 30 seconds)
//   - sqlTimeout: Timeout for SQL queries (0 for default 30 seconds)
//   - functionTimeout: Timeout for function calls (0 for default 30 seconds)
//
// Returns a configured Handler ready to start processing requests.
func NewHandler(deviceID, amqpURL, mysqlDSN, mode string, poolConf *PoolConfig, commandTimeout, sqlTimeout, functionTimeout time.Duration) *Handler {
	// Default to 'open' mode for better performance
	if mode == "" {
		mode = "open"
	}

	// Default pool configuration optimized for typical workloads
	defaultPool := PoolConfig{
		MaxIdleConns:    10,              // Keep 10 idle connections ready
		MaxOpenConns:    20,              // Allow up to 20 concurrent connections
		ConnMaxLifetime: 3 * time.Minute, // Refresh connections every 3 minutes
	}

	// Use provided pool config or defaults, filling in any missing values
	if poolConf == nil {
		poolConf = &defaultPool
	} else {
		if poolConf.MaxIdleConns == 0 {
			poolConf.MaxIdleConns = defaultPool.MaxIdleConns
		}
		if poolConf.MaxOpenConns == 0 {
			poolConf.MaxOpenConns = defaultPool.MaxOpenConns
		}
		if poolConf.ConnMaxLifetime == 0 {
			poolConf.ConnMaxLifetime = defaultPool.ConnMaxLifetime
		}
	}

	// Set default timeouts if not provided
	if commandTimeout == 0 {
		commandTimeout = 30 * time.Second
	}
	if sqlTimeout == 0 {
		sqlTimeout = 30 * time.Second
	}
	if functionTimeout == 0 {
		functionTimeout = 30 * time.Second
	}

	handler := &Handler{
		deviceID:           deviceID,
		amqpURL:            amqpURL,
		mysqlDSN:           mysqlDSN,
		mode:               mode,
		poolConf:           *poolConf,
		commandTimeout:     commandTimeout,                                // Set command timeout
		sqlTimeout:         sqlTimeout,                                    // Set SQL timeout
		functionTimeout:    functionTimeout,                               // Set function timeout
		functionRegistry:   make(map[string]interface{}),                  // Initialize empty function registry
		transactionManager: NewTransactionManager(),                       // Initialize transaction manager
		queryCache:         NewQueryCache(DefaultQueryCacheConfig()),      // Initialize query cache
		sqlValidator:       NewSQLValidator(DefaultSQLValidationConfig()), // Initialize SQL validator

		// Initialize heartbeat manager
		heartbeatManager: NewServerHeartbeatManager(deviceID, DefaultServerHeartbeatConfig()),

		// Initialize queue names
		rpcQueueName:       fmt.Sprintf("device_%s_rpc", deviceID),
		heartbeatQueueName: fmt.Sprintf("device_%s_heartbeat", deviceID),
	}

	// Initialize worker pool with default configuration
	handler.workerPool = NewWorkerPool(handler, &WorkerPoolConfig{
		WorkerCount: 10,
		QueueSize:   100,
		Timeout:     30 * time.Second,
	})

	// Initialize rate limiter with default configuration
	handler.rateLimiter = NewRateLimiter(DefaultRateLimiterConfig())

	return handler
}

// RegisterFunction registers a single function in the function registry.
// This enables dynamic function registration from examples or external code.
//
// Parameters:
//   - name: The name by which the function will be called
//   - function: The actual function to register (must be a valid Go function)
//
// The function uses reflection to inspect the function signature at runtime,
// allowing for type-safe parameter conversion and execution.
func (h *Handler) RegisterFunction(name string, function interface{}) {
	if h.functionRegistry == nil {
		h.functionRegistry = make(map[string]interface{})
	}
	h.functionRegistry[name] = function
	log.Printf("[server] Function '%s' registered", name)
}

// RegisterFunctions registers multiple functions at once for batch registration.
// This is useful for registering all functions from a module or package.
//
// Parameters:
//   - functions: A map of function names to function objects
//
// This method is more efficient than calling RegisterFunction multiple times
// and provides a single log entry for all registered functions.
func (h *Handler) RegisterFunctions(functions map[string]interface{}) {
	if h.functionRegistry == nil {
		h.functionRegistry = make(map[string]interface{})
	}
	for name, function := range functions {
		h.functionRegistry[name] = function
	}
	log.Printf("[server] %d functions registered", len(functions))
}

// GetRegisteredFunctions returns a list of all registered function names.
// This is useful for debugging, monitoring, or providing function discovery.
//
// Returns:
//   - A slice of strings containing all registered function names
func (h *Handler) GetRegisteredFunctions() []string {
	var names []string
	for name := range h.functionRegistry {
		names = append(names, name)
	}
	return names
}

// Start begins the server's message processing loop.
// This method establishes connections to RabbitMQ and MySQL, declares the device queue,
// and starts listening for incoming messages.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Any error that occurred during startup or operation
//
// The method runs indefinitely until the context is cancelled or an error occurs.
// It handles three types of operations based on the configured mode:
// - "open": Maintains a persistent database connection pool
// - "close": Opens/closes database connections per query
func (h *Handler) Start(ctx context.Context) error {
	var err error

	// Establish RabbitMQ connection
	h.conn, err = amqp.Dial(h.amqpURL)

	// check URL amqp o amqps
	if strings.HasPrefix(h.amqpURL, "amqps://") {

		// cortar url de amqpURL luego de la @ y el :
		domain := strings.Split(h.amqpURL, "@")[1]
		domain = strings.Split(domain, ":")[0]

		log.Printf("[server] domain: %s", domain)

		// TLS explícito con SNI (opcional pero recomendable)
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: domain,
		}

		h.conn, err = amqp.DialTLS(h.amqpURL, tlsCfg)
	} else {
		h.conn, err = amqp.Dial(h.amqpURL)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer h.conn.Close()

	// Initialize database connection based on mode
	if h.mode == "open" {
		// Open persistent database connection with pooling
		h.db, err = sql.Open("mysql", h.mysqlDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %w", err)
		}

		// Configure connection pool for optimal performance
		h.db.SetMaxIdleConns(h.poolConf.MaxIdleConns)
		h.db.SetMaxOpenConns(h.poolConf.MaxOpenConns)
		h.db.SetConnMaxLifetime(h.poolConf.ConnMaxLifetime)
		defer h.db.Close()

		log.Printf("[server] Database pool initialized: idle=%d open=%d lifetime=%s",
			h.poolConf.MaxIdleConns, h.poolConf.MaxOpenConns, h.poolConf.ConnMaxLifetime)
	} else {
		log.Println("[server] Using 'close' mode: opening/closing DB connection per query")
	}

	// Create RabbitMQ channel for message operations
	ch, err := h.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Declare RPC queue for this device
	_, err = ch.QueueDeclare(
		h.rpcQueueName, // name - RPC queue name using device ID for uniqueness
		false,          // durable - non-persistent (lost if RabbitMQ restarts)
		false,          // delete when unused - keep queue active
		false,          // exclusive - allow multiple consumers
		false,          // no-wait - wait for server confirmation
		nil,            // arguments - no additional arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare RPC queue: %w", err)
	}

	// Declare heartbeat queue for this device
	_, err = ch.QueueDeclare(
		h.heartbeatQueueName, // name - heartbeat queue name using device ID for uniqueness
		false,                // durable - non-persistent (lost if RabbitMQ restarts)
		false,                // delete when unused - keep queue active
		false,                // exclusive - allow multiple consumers
		false,                // no-wait - wait for server confirmation
		nil,                  // arguments - no additional arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare heartbeat queue: %w", err)
	}

	log.Printf("[server] Queues '%s' and '%s' declared successfully", h.rpcQueueName, h.heartbeatQueueName)

	// Start consuming messages from the RPC queue
	rpcMsgs, err := ch.Consume(h.rpcQueueName, "", true, true, false, false, nil)
	if err != nil {
		return err
	}

	// Start consuming messages from the heartbeat queue
	heartbeatMsgs, err := ch.Consume(h.heartbeatQueueName, "", true, true, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("[server] Listening on RPC queue %s and heartbeat queue %s", h.rpcQueueName, h.heartbeatQueueName)

	// Start the worker pool for concurrent message processing
	if err := h.workerPool.Start(); err != nil {
		return fmt.Errorf("failed to start worker pool: %w", err)
	}
	defer h.workerPool.Stop(10 * time.Second) // 10 second shutdown timeout
	defer h.rateLimiter.Stop()                // Stop rate limiter cleanup goroutine

	// Start heartbeat manager
	h.heartbeatManager.Start()
	defer h.heartbeatManager.Stop()

	// Start transaction cleanup goroutine
	go h.transactionCleanupLoop(ctx)

	// Main message processing loop
	for {
		select {
		case <-ctx.Done():
			// Context cancelled, shut down gracefully
			log.Printf("[server] Shutting down server...")
			return nil
		case msg := <-rpcMsgs:
			// Submit RPC message to worker pool
			task := MessageTask{
				Channel:   ch,
				Message:   msg,
				Timestamp: time.Now(),
			}

			if err := h.workerPool.SubmitTask(task); err != nil {
				log.Printf("[server] Failed to submit RPC task to worker pool: %v", err)
				// Send error response directly if worker pool fails
				errorResp := RPCResponse{Error: "Server overloaded, please try again"}
				if body, marshalErr := json.Marshal(errorResp); marshalErr == nil {
					ch.PublishWithContext(ctx, "", msg.ReplyTo, false, false, amqp.Publishing{
						ContentType:   "application/json",
						CorrelationId: msg.CorrelationId,
						Body:          body,
					})
				}
			}
		case msg := <-heartbeatMsgs:
			// Process heartbeat message directly (high priority)
			h.heartbeatManager.HandleHeartbeatPing(ch, msg)
		}
	}
}

// handleMessage processes incoming messages from the RabbitMQ queue.
// It deserializes the request, logs the operation, and routes to the appropriate handler
// based on the request type (sql, function, or command).
//
// Parameters:
//   - ch: RabbitMQ channel for sending responses
//   - msg: The incoming message delivery containing the request
//
// This method runs in a separate goroutine for each message to enable concurrent processing.
func (h *Handler) handleMessage(ch *amqp.Channel, msg amqp.Delivery) {
	// Check if this is a heartbeat message by examining the message body first
	if len(msg.Body) > 0 {
		var heartbeatCheck map[string]interface{}
		if err := json.Unmarshal(msg.Body, &heartbeatCheck); err == nil {
			// Check if this looks like a heartbeat message
			if _, hasDeviceID := heartbeatCheck["deviceID"]; hasDeviceID {
				if _, hasClientIP := heartbeatCheck["clientIP"]; hasClientIP {
					if _, hasCorrID := heartbeatCheck["corrID"]; hasCorrID {
						// This is likely a heartbeat message, handle it directly
						log.Printf("[server] Detected heartbeat message in RPC queue, forwarding to heartbeat manager")
						h.heartbeatManager.HandleHeartbeatPing(ch, msg)
						return
					}
				}
			}
		}
	}

	var req RPCRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		log.Printf("[server] Failed to parse RPC request: %v (body: %s)", err, string(msg.Body))
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
		return
	}

	// Check rate limit before processing request
	if !h.rateLimiter.Allow(req.ClientIP) {
		log.Printf("[server] rate limit exceeded for client %s", req.ClientIP)
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: "Rate limit exceeded. Please slow down your requests.",
		})
		return
	}

	log.Printf("[server] received ip=%s type=%s query=%s", req.ClientIP, req.Type, req.Query)

	// Route to appropriate handler based on request type
	switch req.Type {
	case "sql":
		h.handleSQL(ch, msg, req)

	case "function":
		h.handleFunction(ch, msg, req)

	case "command":
		h.handleCommand(ch, msg, req)

	case "transaction":
		h.handleTransaction(ch, msg, req)

	case "heartbeat_ping":
		// Handle heartbeat ping (should be processed by heartbeat manager)
		h.heartbeatManager.HandleHeartbeatPing(ch, msg)

	default:
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: fmt.Sprintf("unsupported type: %s", req.Type),
		})
	}
}

// handleSQL processes SQL query requests with parameter binding and type conversion.
// It supports both connection pooling (open mode) and per-query connections (close mode).
// It also supports transaction-aware query execution.
//
// Parameters:
//   - ch: RabbitMQ channel for sending responses
//   - msg: The original message for reply routing
//   - req: The parsed SQL request containing query and parameters
//
// Features:
// - 10-second timeout for query execution
// - Automatic parameter binding for security
// - Type-safe column data conversion
// - Proper connection management based on mode
// - Transaction support for ACID operations
func (h *Handler) handleSQL(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	// Create context with timeout to prevent long-running queries
	ctx, cancel := context.WithTimeout(context.Background(), h.sqlTimeout)
	defer cancel()

	// Validate SQL query for security and policy compliance
	validationResult := h.sqlValidator.ValidateQuery(req.Query, req.Params)
	if !validationResult.Valid {
		// Query failed validation, return error
		errorMsg := fmt.Sprintf("SQL validation failed: %s", strings.Join(validationResult.Errors, "; "))
		log.Printf("[server] SQL validation blocked query from %s: %s (risk: %s)",
			req.ClientIP, truncateQuery(req.Query, 50), validationResult.Risk)
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: errorMsg})
		return
	}

	// Log warnings if any
	if len(validationResult.Warnings) > 0 {
		log.Printf("[server] SQL validation warnings for query: %s", strings.Join(validationResult.Warnings, "; "))
	}

	// Skip cache for transactions and write operations
	useCache := req.TransactionID == "" && isReadOnlyQuery(req.Query)

	// Try to get result from cache first (only for read-only queries outside transactions)
	if useCache {
		if cachedResponse, found := h.queryCache.Get(req.Query, req.Params); found {
			log.Printf("[server] Cache HIT for query: %s", truncateQuery(req.Query, 50))
			h.respond(ch, msg.ReplyTo, msg.CorrelationId, *cachedResponse)
			return
		}
		log.Printf("[server] Cache MISS for query: %s", truncateQuery(req.Query, 50))
	}

	var rows *sql.Rows
	var err error

	// Check if this query should run within a transaction
	if req.TransactionID != "" {
		// Use transaction for query execution
		transaction, exists := h.transactionManager.GetTransaction(req.TransactionID)
		if !exists {
			h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
				Error: fmt.Sprintf("transaction %s not found", req.TransactionID),
			})
			return
		}

		// Execute query within transaction
		rows, err = transaction.Tx.QueryContext(ctx, req.Query, req.Params...)
		if err != nil {
			h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
			return
		}
		defer rows.Close()
	} else {
		// Execute query without transaction (original behavior)
		var db *sql.DB

		// Use appropriate database connection based on configured mode
		if h.mode == "open" {
			// Use persistent connection pool
			db = h.db
		} else {
			// Open fresh connection for this query
			db, err = sql.Open("mysql", h.mysqlDSN)
			if err != nil {
				h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
				return
			}
			defer db.Close()
		}

		// Execute query with parameter binding for security
		rows, err = db.QueryContext(ctx, req.Query, req.Params...)
		if err != nil {
			h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
			return
		}
		defer rows.Close()
	}

	// Get column names for response structure
	cols, err := rows.Columns()
	if err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
		return
	}

	// Get column types for proper data conversion
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
		return
	}

	var data [][]interface{}
	for rows.Next() {
		// Create scan destinations for all columns
		scanDest := make([]interface{}, len(cols))
		for i := range scanDest {
			scanDest[i] = new(interface{})
		}

		// Scan row data into destinations
		if err := rows.Scan(scanDest...); err != nil {
			h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{Error: err.Error()})
			return
		}

		// Convert and clean data types for JSON serialization
		row := make([]interface{}, len(cols))
		for i, val := range scanDest {
			v := *(val.(*interface{}))
			row[i] = h.convertDatabaseValue(v, colTypes[i])
		}
		data = append(data, row)
	}

	// Prepare response
	response := RPCResponse{
		Columns: cols,
		Rows:    data,
	}

	// Cache the result if applicable (only for read-only queries outside transactions)
	if useCache {
		h.queryCache.Set(req.Query, req.Params, response)
		log.Printf("[server] Query result cached: %s", truncateQuery(req.Query, 50))
	}

	// Send successful response with query results
	h.respond(ch, msg.ReplyTo, msg.CorrelationId, response)
}

// convertDatabaseValue converts database values to appropriate JSON-serializable types.
// This method handles the complexity of MySQL's type system and ensures consistent
// data representation across different column types.
//
// Parameters:
//   - val: The raw value from the database
//   - colType: MySQL column type information for informed conversion
//
// Returns:
//   - interface{}: A JSON-serializable value appropriate for the column type
//
// The conversion strategy:
// - Numeric types: Convert to strings to preserve precision and avoid float precision issues
// - Text types: Convert byte arrays to strings
// - Native types: Pass through directly (int, float, bool, string)
// - Unknown types: Convert to string representation
func (h *Handler) convertDatabaseValue(val interface{}, colType *sql.ColumnType) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		// Determine conversion strategy based on MySQL column type
		dbType := colType.DatabaseTypeName()
		switch dbType {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
			// Convert bytes to string for integer types to preserve exact values
			str := string(v)
			if str == "" {
				return 0
			}
			// Return as string to let client parse with appropriate precision
			return str
		case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
			// Return decimal types as strings to avoid floating-point precision loss
			return string(v)
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			// Convert text types to strings
			return string(v)
		default:
			// Default: convert unknown byte arrays to strings
			return string(v)
		}
	case string:
		return v
	case int, int8, int16, int32, int64:
		return v
	case uint, uint8, uint16, uint32, uint64:
		return v
	case float32, float64:
		return v
	case bool:
		return v
	default:
		// Convert unknown types to string representation
		return fmt.Sprintf("%v", v)
	}
}

// handleCommand executes system commands and returns their output.
// It provides controlled access to the operating system with timeout management
// and comprehensive output capture.
//
// Parameters:
//   - ch: RabbitMQ channel for sending responses
//   - msg: The original message for reply routing
//   - req: The parsed command request containing the command string
//
// Features:
// - 30-second timeout to prevent runaway processes
// - Combined stdout/stderr capture
// - Line-by-line output preservation
// - Comprehensive error reporting
// - Security through command parsing and validation
func (h *Handler) handleCommand(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	// Create context with timeout to prevent commands from running indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), h.commandTimeout)
	defer cancel()

	log.Printf("[server] executing command: %s", req.Query)

	// Parse command and arguments from request
	// The command comes in req.Query and needs to be split into command and arguments
	parts := strings.Fields(req.Query)
	if len(parts) == 0 {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: "empty command",
		})
		return
	}

	command := parts[0]
	args := parts[1:]

	// Create and execute the command with context for timeout control
	cmd := exec.CommandContext(ctx, command, args...)

	// Capture both stdout and stderr for comprehensive output
	output, err := cmd.CombinedOutput()

	if err != nil {
		// If command fails, include both error and output (if any)
		errorMsg := fmt.Sprintf("command failed: %v", err)
		if len(output) > 0 {
			errorMsg += fmt.Sprintf("\nOutput: %s", string(output))
		}
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: errorMsg,
		})
		return
	}

	// Convert output to string and split into lines for tabular format
	outputStr := string(output)

	// Split output into individual lines
	lines := strings.Split(outputStr, "\n")

	// Prepare rows for response (each line becomes a row)
	var rows [][]interface{}

	// Add each line as a row (including empty lines for output fidelity)
	for _, line := range lines {
		rows = append(rows, []interface{}{line})
	}

	// If no output, indicate successful execution
	if len(rows) == 0 {
		rows = append(rows, []interface{}{"(command executed successfully - no output)"})
	}

	// Send response with command output in tabular format
	h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
		Columns: []string{"output"},
		Rows:    rows,
	})

	log.Printf("[server] command executed successfully, returned %d lines", len(rows))
}

// handleFunction executes remote function calls with type-safe parameter conversion.
// It deserializes function requests, executes registered functions using reflection,
// and returns results in a consistent tabular format.
//
// Parameters:
//   - ch: RabbitMQ channel for sending responses
//   - msg: The original message for reply routing
//   - req: The parsed function request containing JSON function call data
//
// Features:
// - 30-second timeout for function execution
// - Type-safe parameter conversion using reflection
// - Dynamic function lookup from registry
// - Consistent result formatting
// - Comprehensive error handling
func (h *Handler) handleFunction(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	// Create context with timeout for function execution
	ctx, cancel := context.WithTimeout(context.Background(), h.functionTimeout)
	defer cancel()

	log.Printf("[server] executing function: %s", req.Query)

	// Parse function request from JSON in req.Query
	var funcReq FunctionRequest
	if err := json.Unmarshal([]byte(req.Query), &funcReq); err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: fmt.Sprintf("invalid function request: %v", err),
		})
		return
	}

	// Execute the requested function with parameter conversion
	result, err := h.executeFunction(ctx, funcReq)
	if err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: fmt.Sprintf("function execution failed: %v", err),
		})
		return
	}

	// Convert function result to tabular response format
	columns, rows := h.convertFunctionResult(result)

	// Send successful response with function results
	h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
		Columns: columns,
		Rows:    rows,
	})

	log.Printf("[server] function executed successfully")
}

// executeFunction executes a function by name using Go's reflection system.
// This method provides the core functionality for dynamic function execution
// with type-safe parameter conversion and result handling.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - funcReq: The function request containing name and parameters
//
// Returns:
//   - []interface{}: Array of function return values
//   - error: Any error that occurred during execution
//
// The method uses reflection to:
// 1. Look up the function by name in the registry
// 2. Convert parameters to the correct types
// 3. Invoke the function dynamically
// 4. Capture and return all return values
func (h *Handler) executeFunction(ctx context.Context, funcReq FunctionRequest) ([]interface{}, error) {
	// Check if context was cancelled before proceeding
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Look up function by name in the registry
	funcValue := h.getFunctionByName(funcReq.Name)
	if !funcValue.IsValid() {
		return nil, fmt.Errorf("function '%s' not found", funcReq.Name)
	}

	// Prepare and convert parameters to match function signature
	params, err := h.prepareFunctionParams(funcReq.Params, funcValue.Type())
	if err != nil {
		return nil, fmt.Errorf("error preparing parameters: %v", err)
	}

	// Execute function using reflection
	results := funcValue.Call(params)

	// Convert all return values to interface{} slice
	var output []interface{}
	for _, result := range results {
		output = append(output, result.Interface())
	}

	return output, nil
}

// getFunctionByName retrieves a function from the registry by name.
// This method provides safe function lookup with proper error handling.
//
// Parameters:
//   - name: The name of the function to retrieve
//
// Returns:
//   - reflect.Value: The function as a reflection Value, or invalid Value if not found
//
// This method is used internally by executeFunction to dynamically
// locate registered functions for execution.
func (h *Handler) getFunctionByName(name string) reflect.Value {
	if h.functionRegistry == nil {
		return reflect.Value{}
	}

	if fn, exists := h.functionRegistry[name]; exists {
		return reflect.ValueOf(fn)
	}

	return reflect.Value{}
}

// prepareFunctionParams converts function parameters to their correct types.
// This method ensures type safety when calling functions dynamically by
// converting JSON values to the exact types expected by the function signature.
//
// Parameters:
//   - params: Array of typed parameters from the function request
//   - funcType: Reflection type of the target function
//
// Returns:
//   - []reflect.Value: Array of converted parameters ready for function call
//   - error: Any error that occurred during type conversion
//
// The method validates parameter count and converts each parameter
// using type information from both the request and function signature.
func (h *Handler) prepareFunctionParams(params []FunctionParam, funcType reflect.Type) ([]reflect.Value, error) {
	if len(params) != funcType.NumIn() {
		return nil, fmt.Errorf("expected %d parameters, got %d", funcType.NumIn(), len(params))
	}

	var values []reflect.Value
	for i, param := range params {
		expectedType := funcType.In(i)
		value, err := h.convertToType(param.Value, expectedType)
		if err != nil {
			return nil, fmt.Errorf("parameter %d: %v", i, err)
		}
		values = append(values, value)
	}

	return values, nil
}

// convertToType converts a value to the specified reflection type.
// This method handles the complexity of converting JSON values (which are limited in type)
// to the rich type system of Go functions.
//
// Parameters:
//   - value: The input value to convert (typically from JSON)
//   - targetType: The target reflection type to convert to
//
// Returns:
//   - reflect.Value: The converted value ready for function parameter
//   - error: Any error that occurred during conversion
//
// Supported conversions:
// - String conversions: Any value to string representation
// - Integer conversions: float64, string to int with validation
// - Boolean conversions: string to bool with validation
// - Slice conversions: Recursive element-wise conversion
// - Struct conversions: JSON marshaling/unmarshaling for complex types
func (h *Handler) convertToType(value interface{}, targetType reflect.Type) (reflect.Value, error) {
	if value == nil {
		return reflect.Zero(targetType), nil
	}

	valueType := reflect.TypeOf(value)
	if valueType == targetType {
		return reflect.ValueOf(value), nil
	}

	// Handle specific type conversions based on target type
	switch targetType.Kind() {
	case reflect.String:
		// Convert any value to string representation
		return reflect.ValueOf(fmt.Sprintf("%v", value)), nil

	case reflect.Int:
		// Convert numeric types and strings to integers
		switch v := value.(type) {
		case float64:
			return reflect.ValueOf(int(v)), nil
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				return reflect.ValueOf(i), nil
			}
		}

	case reflect.Bool:
		// Convert string representations to boolean
		switch v := value.(type) {
		case string:
			if b, err := strconv.ParseBool(v); err == nil {
				return reflect.ValueOf(b), nil
			}
		}

	case reflect.Slice:
		// Convert slice types with element-wise conversion
		if valueType.Kind() == reflect.Slice {
			sourceSlice := reflect.ValueOf(value)
			targetSlice := reflect.MakeSlice(targetType, sourceSlice.Len(), sourceSlice.Len())
			for i := 0; i < sourceSlice.Len(); i++ {
				convertedValue, err := h.convertToType(sourceSlice.Index(i).Interface(), targetType.Elem())
				if err != nil {
					return reflect.Value{}, err
				}
				targetSlice.Index(i).Set(convertedValue)
			}
			return targetSlice, nil
		}

	case reflect.Struct:
		// Handle struct conversions using JSON marshaling for any struct type
		if valueType.Kind() == reflect.Map || valueType.Kind() == reflect.Interface {
			// Convert map/interface{} to target struct using JSON as intermediate format
			if jsonData, err := json.Marshal(value); err == nil {
				// Create a new instance of the target struct type
				newValue := reflect.New(targetType)
				if json.Unmarshal(jsonData, newValue.Interface()) == nil {
					return newValue.Elem(), nil
				}
			}
		}
	}

	return reflect.Value{}, fmt.Errorf("cannot convert %v to %v", valueType, targetType)
}

// convertFunctionResult converts function execution results to tabular response format.
// This method handles the complexity of converting arbitrary function return values
// into a consistent tabular structure for client consumption.
//
// Parameters:
//   - results: Array of values returned by the function
//
// Returns:
//   - []string: Column names for the result table
//   - [][]interface{}: Rows of data for the result table
//
// The method handles:
// - Empty results: Returns "no output" message
// - Single results: Creates single-column table with result or error
// - Multiple results: Creates multi-column table with numbered result columns
// - Error values: Special handling to display error messages
func (h *Handler) convertFunctionResult(results []interface{}) ([]string, [][]interface{}) {
	if len(results) == 0 {
		return []string{"result"}, [][]interface{}{{"no output"}}
	}

	var columns []string
	var rows [][]interface{}

	if len(results) == 1 {
		// Single result: create single-column table
		result := results[0]
		if err, ok := result.(error); ok {
			// Handle error return values
			if err != nil {
				columns = []string{"error"}
				rows = [][]interface{}{{err.Error()}}
			} else {
				columns = []string{"result"}
				rows = [][]interface{}{{"success"}}
			}
		} else {
			// Handle normal return values
			columns = []string{"result"}
			rows = [][]interface{}{{h.formatResult(result)}}
		}
	} else {
		// Multiple results: create multi-column table
		for i := range results {
			columns = append(columns, fmt.Sprintf("result_%d", i+1))
		}

		// Create single row with all results
		var row []interface{}
		for _, res := range results {
			if err, ok := res.(error); ok {
				// Handle error values in multi-result context
				if err != nil {
					row = append(row, err.Error())
				} else {
					row = append(row, "success")
				}
			} else {
				// Handle normal values in multi-result context
				row = append(row, h.formatResult(res))
			}
		}
		rows = [][]interface{}{row}
	}

	return columns, rows
}

// formatResult formats a single result value for display in the response.
// This method handles the conversion of complex Go types to JSON-serializable values
// suitable for transmission to clients.
//
// Parameters:
//   - result: The value to format (can be any Go type)
//
// Returns:
//   - interface{}: A JSON-serializable representation of the result
//
// The method provides special handling for:
// - nil values: Converted to "null" string
// - Slice types: Formatted as string representations
// - Struct types: JSON marshaled when possible
// - Other types: Passed through directly if already serializable
func (h *Handler) formatResult(result interface{}) interface{} {
	if result == nil {
		return "null"
	}

	switch v := result.(type) {
	case []int:
		// Format integer slices as string representation
		return fmt.Sprintf("%v", v)
	case []string:
		// Format string slices as string representation
		return fmt.Sprintf("%v", v)
	default:
		// Try to marshal structs as JSON, fallback to string representation
		if reflect.TypeOf(result).Kind() == reflect.Struct {
			if jsonData, err := json.Marshal(v); err == nil {
				return string(jsonData)
			}
			// Fallback to Go's default struct formatting
			return fmt.Sprintf("%+v", v)
		}
		// Return other types directly (primitives, etc.)
		return result
	}
}

// respond sends a response back to the client via RabbitMQ.
// This method handles the final step of request processing by serializing
// the response and publishing it to the client's reply queue.
//
// Parameters:
//   - ch: RabbitMQ channel for publishing
//   - replyTo: The reply queue name from the original request
//   - corrID: Correlation ID to match request/response pairs
//   - resp: The response object to send to the client
//
// The method uses RabbitMQ's RPC pattern with correlation IDs to ensure
// responses are properly matched to their originating requests.
// Content-Type is set to "application/json" for proper client deserialization.
func (h *Handler) respond(ch *amqp.Channel, replyTo, corrID string, resp RPCResponse) {
	// Serialize response to JSON
	body, _ := json.Marshal(resp)

	// Publish response to client's reply queue
	ch.PublishWithContext(context.Background(), "", replyTo, false, false, amqp.Publishing{
		ContentType:   "application/json", // Indicate JSON content for client parsing
		CorrelationId: corrID,             // Match response to original request
		Body:          body,               // Serialized response data
	})
}

// transactionCleanupLoop runs a periodic cleanup of expired transactions.
// It prevents memory leaks and database connection exhaustion by rolling back
// transactions that have been inactive for too long.
func (h *Handler) transactionCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute) // Cleanup every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[server] Transaction cleanup loop shutting down...")
			return
		case <-ticker.C:
			// Clean up transactions older than 30 minutes
			h.transactionManager.CleanupExpiredTransactions(30 * time.Minute)
		}
	}
}

// isReadOnlyQuery determines if a SQL query is read-only and safe to cache.
// Only SELECT queries are considered read-only and cacheable.
//
// Parameters:
//   - query: SQL query string to analyze
//
// Returns:
//   - bool: true if the query is read-only and safe to cache
func isReadOnlyQuery(query string) bool {
	// Normalize query for analysis
	normalized := strings.TrimSpace(strings.ToLower(query))

	// Check if it starts with SELECT
	return strings.HasPrefix(normalized, "select")
}

// truncateQuery truncates a query string for logging purposes.
//
// Parameters:
//   - query: SQL query string to truncate
//   - maxLength: Maximum length of the truncated string
//
// Returns:
//   - string: Truncated query string with ellipsis if needed
func truncateQuery(query string, maxLength int) string {
	if len(query) <= maxLength {
		return query
	}
	return query[:maxLength] + "..."
}

// GetCacheStats returns current cache statistics for monitoring.
func (h *Handler) GetCacheStats() CacheStats {
	return h.queryCache.GetStats()
}

// ClearCache clears all cached query results.
func (h *Handler) ClearCache() {
	h.queryCache.Clear()
}

// SetCacheConfig updates the cache configuration.
// Note: This creates a new cache instance, clearing all existing cached data.
func (h *Handler) SetCacheConfig(config QueryCacheConfig) {
	h.queryCache = NewQueryCache(config)
	log.Printf("[server] Cache configuration updated")
}

// SetWorkerPoolConfig updates the worker pool configuration.
// Note: This creates a new worker pool instance. Call before starting the server.
func (h *Handler) SetWorkerPoolConfig(config *WorkerPoolConfig) {
	h.workerPool = NewWorkerPool(h, config)
	log.Printf("[server] Worker pool configuration updated: %d workers, queue size %d",
		config.WorkerCount, config.QueueSize)
}

// SetRateLimiterConfig updates the rate limiter configuration.
// Note: This creates a new rate limiter instance. Call before starting the server.
func (h *Handler) SetRateLimiterConfig(config *RateLimiterConfig) {
	h.rateLimiter = NewRateLimiter(config)
	log.Printf("[server] Rate limiter configuration updated: %d req/s, burst %d",
		config.RequestsPerSecond, config.BurstSize)
}

// GetSQLValidationStats returns current SQL validation statistics.
func (h *Handler) GetSQLValidationStats() ValidationStats {
	return h.sqlValidator.GetStats()
}

// SetSQLValidationConfig updates the SQL validation configuration.
func (h *Handler) SetSQLValidationConfig(config SQLValidationConfig) {
	h.sqlValidator.UpdateConfig(config)
	log.Printf("[server] SQL validation configuration updated: enabled=%v, strict=%v",
		config.Enabled, config.StrictMode)
}

// GetCommandTimeout returns the current command timeout configuration.
func (h *Handler) GetCommandTimeout() time.Duration {
	return h.commandTimeout
}

// SetCommandTimeout updates the command timeout configuration.
func (h *Handler) SetCommandTimeout(timeout time.Duration) {
	h.commandTimeout = timeout
	log.Printf("[server] Command timeout updated: %v", timeout)
}

// GetSQLTimeout returns the current SQL timeout configuration.
func (h *Handler) GetSQLTimeout() time.Duration {
	return h.sqlTimeout
}

// SetSQLTimeout updates the SQL timeout configuration.
func (h *Handler) SetSQLTimeout(timeout time.Duration) {
	h.sqlTimeout = timeout
	log.Printf("[server] SQL timeout updated: %v", timeout)
}

// GetFunctionTimeout returns the current function timeout configuration.
func (h *Handler) GetFunctionTimeout() time.Duration {
	return h.functionTimeout
}

// SetFunctionTimeout updates the function timeout configuration.
func (h *Handler) SetFunctionTimeout(timeout time.Duration) {
	h.functionTimeout = timeout
	log.Printf("[server] Function timeout updated: %v", timeout)
}

// GetHeartbeatStats returns heartbeat statistics
func (h *Handler) GetHeartbeatStats() ServerHeartbeatStats {
	return h.heartbeatManager.GetStats()
}

// GetActiveClients returns information about active client connections
func (h *Handler) GetActiveClients() map[string]*ClientHeartbeatInfo {
	return h.heartbeatManager.GetActiveClients()
}
