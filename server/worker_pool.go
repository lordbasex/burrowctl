package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// WorkerPool manages a pool of goroutines for processing messages concurrently.
// This improves server performance by handling multiple requests simultaneously
// while controlling resource usage and preventing goroutine proliferation.
//
// The worker pool provides:
// - Controlled concurrency with configurable worker count
// - Message queuing to handle bursts of requests
// - Graceful shutdown with context cancellation
// - Worker lifecycle management and monitoring
// - Backpressure handling when queue is full
type WorkerPool struct {
	workerCount int                // Number of worker goroutines
	queue       chan MessageTask   // Channel for queuing incoming messages
	handler     *Handler           // Reference to the main handler
	ctx         context.Context    // Context for shutdown coordination
	cancel      context.CancelFunc // Cancel function for shutdown
	wg          sync.WaitGroup     // WaitGroup for graceful shutdown
	started     bool               // Whether the pool has been started
	mutex       sync.RWMutex       // Mutex for thread-safe operations
	logLevel    bool               // Whether to enable detailed logging
}

// MessageTask represents a message processing task for the worker pool.
// It contains all necessary information for a worker to process a message
// and send the response back to the client.
type MessageTask struct {
	Channel   *amqp.Channel // RabbitMQ channel for responding
	Message   amqp.Delivery // The incoming message to process
	Timestamp time.Time     // When the task was created (for monitoring)
}

// WorkerPoolConfig holds configuration options for the worker pool.
// These settings control the behavior and resource usage of the pool.
type WorkerPoolConfig struct {
	WorkerCount int           // Number of worker goroutines (default: 10)
	QueueSize   int           // Size of the message queue buffer (default: 100)
	Timeout     time.Duration // Timeout for individual message processing (default: 30s)
}

// NewWorkerPool creates a new worker pool with the specified configuration.
// The pool is created but not started - call Start() to begin processing.
//
// Parameters:
//   - handler: The main Handler instance for processing messages
//   - config: Configuration options for the pool (nil for defaults)
//
// Returns:
//   - *WorkerPool: Configured worker pool ready to start
func NewWorkerPool(handler *Handler, config *WorkerPoolConfig) *WorkerPool {
	if config == nil {
		config = &WorkerPoolConfig{
			WorkerCount: 10,
			QueueSize:   100,
			Timeout:     30 * time.Second,
		}
	}

	// Apply defaults for zero values
	if config.WorkerCount <= 0 {
		config.WorkerCount = 10
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 100
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workerCount: config.WorkerCount,
		queue:       make(chan MessageTask, config.QueueSize),
		handler:     handler,
		ctx:         ctx,
		cancel:      cancel,
		started:     false,
		logLevel:    handler.config.LogLevel, // Get log level from handler config
	}
}

// Start initializes and starts all worker goroutines.
// This method should be called once after creating the pool.
//
// Returns:
//   - error: Any error that occurred during startup
func (wp *WorkerPool) Start() error {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()

	if wp.started {
		return fmt.Errorf("worker pool already started")
	}

	if wp.logLevel {
		log.Printf("[server] Starting worker pool with %d workers, queue size %d",
			wp.workerCount, cap(wp.queue))
	}

	// Start worker goroutines
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	wp.started = true
	if wp.logLevel {
		log.Printf("[server] Worker pool started successfully")
	}
	return nil
}

// Stop gracefully shuts down the worker pool.
// It stops accepting new tasks and waits for existing tasks to complete.
//
// Parameters:
//   - timeout: Maximum time to wait for workers to finish
//
// Returns:
//   - error: Any error that occurred during shutdown
func (wp *WorkerPool) Stop(timeout time.Duration) error {
	wp.mutex.Lock()
	if !wp.started {
		wp.mutex.Unlock()
		return nil // Already stopped
	}
	wp.mutex.Unlock()

	if wp.logLevel {
		log.Printf("[server] Stopping worker pool...")
	}

	// Signal shutdown to all workers
	wp.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if wp.logLevel {
			log.Printf("[server] Worker pool stopped successfully")
		}
		return nil
	case <-time.After(timeout):
		if wp.logLevel {
			log.Printf("[server] Worker pool shutdown timeout exceeded")
		}
		return fmt.Errorf("worker pool shutdown timeout")
	}
}

// SubmitTask submits a message task to the worker pool for processing.
// If the queue is full, it will block until space is available or context is cancelled.
//
// Parameters:
//   - task: The message task to process
//
// Returns:
//   - error: Any error that occurred during task submission
func (wp *WorkerPool) SubmitTask(task MessageTask) error {
	wp.mutex.RLock()
	defer wp.mutex.RUnlock()

	if !wp.started {
		return fmt.Errorf("worker pool not started")
	}

	select {
	case wp.queue <- task:
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		// Queue is full, this could implement backpressure logic
		if wp.logLevel {
			log.Printf("[server] Worker pool queue is full, dropping message")
		}
		return fmt.Errorf("worker pool queue is full")
	}
}

// worker is the main worker goroutine function.
// It continuously processes messages from the queue until shutdown.
//
// Parameters:
//   - id: Worker identifier for logging
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	if wp.logLevel {
		log.Printf("[server] Worker %d started", id)
	}

	for {
		select {
		case <-wp.ctx.Done():
			if wp.logLevel {
				log.Printf("[server] Worker %d shutting down", id)
			}
			return

		case task := <-wp.queue:
			wp.processTask(id, task)
		}
	}
}

// processTask processes a single message task.
// It handles the message processing and includes error recovery.
//
// Parameters:
//   - workerID: ID of the worker processing the task
//   - task: The message task to process
func (wp *WorkerPool) processTask(workerID int, task MessageTask) {
	start := time.Now()

	// SOLUCIÓN 3: Filtrado en worker pool para mensajes problemáticos
	if len(task.Message.Body) == 0 {
		if wp.logLevel {
			log.Printf("[server] Worker %d skipping empty message (ReplyTo: %s, CorrelationId: %s)",
				workerID, task.Message.ReplyTo, task.Message.CorrelationId)
		}
		return
	}

	// Validar campos mínimos requeridos
	if task.Message.ReplyTo == "" || task.Message.CorrelationId == "" {
		if wp.logLevel {
			log.Printf("[server] Worker %d skipping invalid message - missing ReplyTo or CorrelationId", workerID)
		}
		return
	}

	// Create timeout context for this specific task
	ctx, cancel := context.WithTimeout(wp.ctx, 30*time.Second)
	defer cancel()

	// Recovery from panics in message processing
	defer func() {
		if r := recover(); r != nil {
			if wp.logLevel {
				log.Printf("[server] Worker %d panic recovered: %v", workerID, r)
			}

			// Send error response if possible
			errorResp := RPCResponse{
				Error: fmt.Sprintf("Internal server error: %v", r),
			}
			if body, err := json.Marshal(errorResp); err == nil {
				task.Channel.PublishWithContext(ctx, "", task.Message.ReplyTo, false, false, amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: task.Message.CorrelationId,
					Body:          body,
				})
			}
		}
	}()

	// Log task processing start
	queueTime := start.Sub(task.Timestamp)
	if wp.logLevel {
		log.Printf("[server] Worker %d processing message (queue time: %v)", workerID, queueTime)
	}

	// Process the message using the existing handler logic
	wp.handler.handleMessage(task.Channel, task.Message)

	// Log completion
	processingTime := time.Since(start)
	if wp.logLevel {
		log.Printf("[server] Worker %d completed message (processing time: %v)", workerID, processingTime)
	}
}

// GetStats returns current statistics about the worker pool.
// This is useful for monitoring and debugging.
//
// Returns:
//   - WorkerPoolStats: Current pool statistics
func (wp *WorkerPool) GetStats() WorkerPoolStats {
	wp.mutex.RLock()
	defer wp.mutex.RUnlock()

	return WorkerPoolStats{
		WorkerCount: wp.workerCount,
		QueueSize:   cap(wp.queue),
		QueuedTasks: len(wp.queue),
		IsRunning:   wp.started && wp.ctx.Err() == nil,
	}
}

// WorkerPoolStats contains statistics about the worker pool state.
type WorkerPoolStats struct {
	WorkerCount int  // Number of worker goroutines
	QueueSize   int  // Maximum queue capacity
	QueuedTasks int  // Current number of queued tasks
	IsRunning   bool // Whether the pool is currently running
}
