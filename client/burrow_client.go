// Package client provides an extended BurrowClient that wraps the standard database/sql
// interface with specialized methods for different operation types.
package client

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// BurrowClient provides an extended interface for burrowctl operations
// with specialized methods for SQL queries, system commands, and function calls.
type BurrowClient struct {
	db *sql.DB
}

// NewBurrowClient creates a new BurrowClient wrapping a standard sql.DB connection.
// This provides a cleaner interface for different operation types while maintaining
// compatibility with the existing database/sql driver.
func NewBurrowClient(dsn string) (*BurrowClient, error) {
	db, err := sql.Open("rabbitsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open burrow connection: %w", err)
	}

	return &BurrowClient{db: db}, nil
}

// DB returns the underlying sql.DB instance for direct access to standard database operations.
// This maintains compatibility with existing code that uses the database/sql interface.
func (bc *BurrowClient) DB() *sql.DB {
	return bc.db
}

// Close closes the underlying database connection.
func (bc *BurrowClient) Close() error {
	return bc.db.Close()
}

// Ping verifies the connection is still alive.
func (bc *BurrowClient) Ping() error {
	return bc.db.Ping()
}

// Query executes a standard SQL query with parameter binding.
// This is equivalent to db.Query() but provides a cleaner interface.
func (bc *BurrowClient) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return bc.db.Query(query, args...)
}

// QueryRow executes a SQL query that is expected to return at most one row.
func (bc *BurrowClient) QueryRow(query string, args ...interface{}) *sql.Row {
	return bc.db.QueryRow(query, args...)
}

// Exec executes a SQL statement that doesn't return rows.
func (bc *BurrowClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	return bc.db.Exec(query, args...)
}

// Begin starts a transaction.
func (bc *BurrowClient) Begin() (*sql.Tx, error) {
	return bc.db.Begin()
}

// Prepare creates a prepared statement.
func (bc *BurrowClient) Prepare(query string) (*sql.Stmt, error) {
	return bc.db.Prepare(query)
}

// CommandResult represents the result of a system command execution.
type CommandResult struct {
	Command    string    `json:"command"`
	ExitCode   int       `json:"exit_code"`
	Stdout     []string  `json:"stdout"`
	Stderr     []string  `json:"stderr"`
	ExecutedAt time.Time `json:"executed_at"`
}

// ExecCommand executes a system command on the remote server.
// This provides a cleaner interface than using db.Query("COMMAND:...").
func (bc *BurrowClient) ExecCommand(command string) (*CommandResult, error) {
	rows, err := bc.db.Query("COMMAND:" + command)
	if err != nil {
		return nil, fmt.Errorf("command execution failed: %w", err)
	}
	defer rows.Close()

	result := &CommandResult{
		Command:    command,
		ExecutedAt: time.Now(),
		Stdout:     make([]string, 0),
		Stderr:     make([]string, 0),
	}

	// Process command output
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("failed to scan command output: %w", err)
		}

		// Simple heuristic: if line contains "error" or "ERROR", treat as stderr
		if strings.Contains(strings.ToLower(line), "error") {
			result.Stderr = append(result.Stderr, line)
		} else {
			result.Stdout = append(result.Stdout, line)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading command results: %w", err)
	}

	return result, nil
}

// FunctionParam represents a typed parameter for function calls.
type FunctionParam struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// FunctionRequest represents a function call request.
type FunctionRequest struct {
	Name   string          `json:"name"`
	Params []FunctionParam `json:"params"`
}

// FunctionResult represents the result of a function execution.
type FunctionResult struct {
	Function   string        `json:"function"`
	Result     interface{}   `json:"result"`
	Error      string        `json:"error,omitempty"`
	ExecutedAt time.Time     `json:"executed_at"`
	Duration   time.Duration `json:"duration"`
}

// ExecFunction executes a custom function on the remote server.
// This provides a cleaner interface than using db.Query("FUNCTION:...").
func (bc *BurrowClient) ExecFunction(name string, params ...FunctionParam) (*FunctionResult, error) {
	start := time.Now()

	funcReq := FunctionRequest{
		Name:   name,
		Params: params,
	}

	jsonData, err := json.Marshal(funcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal function request: %w", err)
	}

	rows, err := bc.db.Query("FUNCTION:" + string(jsonData))
	if err != nil {
		return &FunctionResult{
			Function:   name,
			Error:      err.Error(),
			ExecutedAt: start,
			Duration:   time.Since(start),
		}, fmt.Errorf("function execution failed: %w", err)
	}
	defer rows.Close()

	result := &FunctionResult{
		Function:   name,
		ExecutedAt: start,
		Duration:   time.Since(start),
	}

	// Get column information to understand the result structure
	columns, err := rows.Columns()
	if err != nil {
		result.Error = err.Error()
		return result, fmt.Errorf("failed to get columns: %w", err)
	}

	// Process function result
	if rows.Next() {
		// Create scan destinations for all columns
		scanDest := make([]interface{}, len(columns))
		for i := range scanDest {
			scanDest[i] = new(interface{})
		}

		// Scan all columns
		if err := rows.Scan(scanDest...); err != nil {
			result.Error = err.Error()
			return result, fmt.Errorf("failed to scan function result: %w", err)
		}

		// Convert scan destinations to actual values
		values := make([]interface{}, len(columns))
		for i, dest := range scanDest {
			values[i] = *(dest.(*interface{}))
		}

		// Handle different result formats based on column count
		if len(columns) == 1 {
			// Single result: use the first value
			result.Result = values[0]
		} else {
			// Multiple results: create a map with column names as keys
			resultMap := make(map[string]interface{})
			for i, col := range columns {
				resultMap[col] = values[i]
			}
			result.Result = resultMap
		}
	}

	if err := rows.Err(); err != nil {
		result.Error = err.Error()
		return result, fmt.Errorf("error reading function results: %w", err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// Helper functions for common parameter types

// StringParam creates a string parameter for function calls.
func StringParam(value string) FunctionParam {
	return FunctionParam{Type: "string", Value: value}
}

// IntParam creates an integer parameter for function calls.
func IntParam(value int) FunctionParam {
	return FunctionParam{Type: "int", Value: value}
}

// Int64Param creates an int64 parameter for function calls.
func Int64Param(value int64) FunctionParam {
	return FunctionParam{Type: "int64", Value: value}
}

// Float64Param creates a float64 parameter for function calls.
func Float64Param(value float64) FunctionParam {
	return FunctionParam{Type: "float64", Value: value}
}

// BoolParam creates a boolean parameter for function calls.
func BoolParam(value bool) FunctionParam {
	return FunctionParam{Type: "bool", Value: value}
}

// JSONParam creates a JSON parameter for function calls.
func JSONParam(value interface{}) FunctionParam {
	return FunctionParam{Type: "json", Value: value}
}
