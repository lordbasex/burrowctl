package server

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TransactionManager manages database transactions across multiple requests.
// It maintains a registry of active transactions and provides thread-safe
// access to transaction instances.
type TransactionManager struct {
	transactions map[string]*Transaction // Active transactions indexed by transaction ID
	mutex        sync.RWMutex            // Thread-safe access to transactions map
	logLevel     bool                    // Whether to enable detailed logging
}

// Transaction represents an active database transaction.
// It maintains the transaction state, database connection, and metadata.
type Transaction struct {
	ID        string       // Unique transaction identifier
	Tx        *sql.Tx      // Database transaction instance
	StartTime time.Time    // When the transaction was started
	LastUsed  time.Time    // Last time the transaction was used
	mutex     sync.RWMutex // Thread-safe access to transaction state
}

// NewTransactionManager creates a new transaction manager instance.
func NewTransactionManager(logLevel bool) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		logLevel:     logLevel,
	}
}

// BeginTransaction starts a new database transaction.
//
// Parameters:
//   - transactionID: Unique identifier for the transaction
//   - db: Database connection to use for the transaction
//
// Returns:
//   - *Transaction: The new transaction instance
//   - error: Any error that occurred during transaction start
func (tm *TransactionManager) BeginTransaction(transactionID string, db *sql.DB) (*Transaction, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Check if transaction already exists
	if _, exists := tm.transactions[transactionID]; exists {
		return nil, fmt.Errorf("transaction %s already exists", transactionID)
	}

	// Start database transaction
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin database transaction: %v", err)
	}

	// Create transaction instance
	transaction := &Transaction{
		ID:        transactionID,
		Tx:        tx,
		StartTime: time.Now(),
		LastUsed:  time.Now(),
	}

	// Register transaction
	tm.transactions[transactionID] = transaction

	if tm.logLevel {
		log.Printf("[server] Transaction started: %s", transactionID)
	}
	return transaction, nil
}

// GetTransaction retrieves an existing transaction by ID.
//
// Parameters:
//   - transactionID: Unique identifier for the transaction
//
// Returns:
//   - *Transaction: The transaction instance if found
//   - bool: Whether the transaction was found
func (tm *TransactionManager) GetTransaction(transactionID string) (*Transaction, bool) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	transaction, exists := tm.transactions[transactionID]
	if exists {
		transaction.mutex.Lock()
		transaction.LastUsed = time.Now()
		transaction.mutex.Unlock()
	}

	return transaction, exists
}

// CommitTransaction commits a transaction and removes it from the registry.
//
// Parameters:
//   - transactionID: Unique identifier for the transaction
//
// Returns:
//   - error: Any error that occurred during commit
func (tm *TransactionManager) CommitTransaction(transactionID string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	transaction, exists := tm.transactions[transactionID]
	if !exists {
		return fmt.Errorf("transaction %s not found", transactionID)
	}

	// Commit the database transaction
	err := transaction.Tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction %s: %v", transactionID, err)
	}

	// Remove from registry
	delete(tm.transactions, transactionID)

	duration := time.Since(transaction.StartTime)
	if tm.logLevel {
		log.Printf("[server] Transaction committed: %s (duration: %v)", transactionID, duration)
	}
	return nil
}

// RollbackTransaction rolls back a transaction and removes it from the registry.
//
// Parameters:
//   - transactionID: Unique identifier for the transaction
//
// Returns:
//   - error: Any error that occurred during rollback
func (tm *TransactionManager) RollbackTransaction(transactionID string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	transaction, exists := tm.transactions[transactionID]
	if !exists {
		return fmt.Errorf("transaction %s not found", transactionID)
	}

	// Rollback the database transaction
	err := transaction.Tx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to rollback transaction %s: %v", transactionID, err)
	}

	// Remove from registry
	delete(tm.transactions, transactionID)

	duration := time.Since(transaction.StartTime)
	if tm.logLevel {
		log.Printf("[server] Transaction rolled back: %s (duration: %v)", transactionID, duration)
	}
	return nil
}

// CleanupExpiredTransactions removes transactions that have been inactive for too long.
// This prevents memory leaks and database connection exhaustion.
//
// Parameters:
//   - maxAge: Maximum age for inactive transactions
func (tm *TransactionManager) CleanupExpiredTransactions(maxAge time.Duration) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	now := time.Now()
	var expiredIDs []string

	// Find expired transactions
	for id, transaction := range tm.transactions {
		transaction.mutex.RLock()
		if now.Sub(transaction.LastUsed) > maxAge {
			expiredIDs = append(expiredIDs, id)
		}
		transaction.mutex.RUnlock()
	}

	// Clean up expired transactions
	for _, id := range expiredIDs {
		transaction := tm.transactions[id]

		// Force rollback the database transaction
		if err := transaction.Tx.Rollback(); err != nil {
			if tm.logLevel {
				log.Printf("[server] Error rolling back expired transaction %s: %v", id, err)
			}
		}

		// Remove from registry
		delete(tm.transactions, id)

		duration := time.Since(transaction.StartTime)
		if tm.logLevel {
			log.Printf("[server] Expired transaction cleaned up: %s (duration: %v)", id, duration)
		}
	}
}

// SetLogLevel updates the log level for the transaction manager
func (tm *TransactionManager) SetLogLevel(logLevel bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.logLevel = logLevel
}

// GetStats returns statistics about active transactions.
func (tm *TransactionManager) GetStats() map[string]interface{} {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	stats := map[string]interface{}{
		"active_transactions": len(tm.transactions),
		"transactions":        make([]map[string]interface{}, 0),
	}

	for id, transaction := range tm.transactions {
		transaction.mutex.RLock()
		txStats := map[string]interface{}{
			"id":        id,
			"duration":  time.Since(transaction.StartTime).String(),
			"last_used": transaction.LastUsed.Format(time.RFC3339),
		}
		transaction.mutex.RUnlock()

		stats["transactions"] = append(stats["transactions"].([]map[string]interface{}), txStats)
	}

	return stats
}

// handleTransaction processes transaction control commands (BEGIN, COMMIT, ROLLBACK).
//
// Parameters:
//   - ch: RabbitMQ channel for sending responses
//   - msg: The original message for reply routing
//   - req: The parsed transaction request
func (h *Handler) handleTransaction(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	switch req.Command {
	case "BEGIN":
		h.handleBeginTransaction(ch, msg, req)
	case "COMMIT":
		h.handleCommitTransaction(ch, msg, req)
	case "ROLLBACK":
		h.handleRollbackTransaction(ch, msg, req)
	default:
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: fmt.Sprintf("unsupported transaction command: %s", req.Command),
		})
	}
}

// handleBeginTransaction starts a new transaction.
func (h *Handler) handleBeginTransaction(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	var db *sql.DB
	var err error

	// Get database connection
	if h.mode == "open" {
		db = h.db
	} else {
		db, err = sql.Open("mysql", h.mysqlDSN)
		if err != nil {
			h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
				Error: fmt.Sprintf("failed to open database connection: %v", err),
			})
			return
		}
		// Note: We don't close the connection here as it's needed for the transaction
	}

	// Start transaction
	_, err = h.transactionManager.BeginTransaction(req.TransactionID, db)
	if err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: err.Error(),
		})
		return
	}

	// Send success response
	h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"BEGIN"}},
	})
}

// handleCommitTransaction commits an existing transaction.
func (h *Handler) handleCommitTransaction(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	err := h.transactionManager.CommitTransaction(req.TransactionID)
	if err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: err.Error(),
		})
		return
	}

	// Send success response
	h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"COMMIT"}},
	})
}

// handleRollbackTransaction rolls back an existing transaction.
func (h *Handler) handleRollbackTransaction(ch *amqp.Channel, msg amqp.Delivery, req RPCRequest) {
	err := h.transactionManager.RollbackTransaction(req.TransactionID)
	if err != nil {
		h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
			Error: err.Error(),
		})
		return
	}

	// Send success response
	h.respond(ch, msg.ReplyTo, msg.CorrelationId, RPCResponse{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"ROLLBACK"}},
	})
}
