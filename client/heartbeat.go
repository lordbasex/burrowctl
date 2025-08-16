package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HeartbeatConfig holds configuration for heartbeat and connection monitoring
type HeartbeatConfig struct {
	Enabled         bool          // Whether heartbeat is enabled
	Interval        time.Duration // How often to send heartbeats
	Timeout         time.Duration // How long to wait for heartbeat response
	MaxMissedBeats  int           // Maximum missed heartbeats before considering connection dead
	DisconnectDelay time.Duration // Delay before disconnecting after missed heartbeats
	LogLevel        bool          // Whether to enable detailed logging
}

// DefaultHeartbeatConfig returns sensible default heartbeat configuration
func DefaultHeartbeatConfig() *HeartbeatConfig {
	return &HeartbeatConfig{
		Enabled:         true,
		Interval:        30 * time.Second, // Send heartbeat every 30 seconds
		Timeout:         10 * time.Second, // Wait 10 seconds for response
		MaxMissedBeats:  3,                // Allow 3 missed heartbeats
		DisconnectDelay: 5 * time.Second,  // Wait 5 seconds before disconnecting
		LogLevel:        false,            // Default: minimal logging
	}
}

// HeartbeatManager handles client-side heartbeat monitoring with separate queues
type HeartbeatManager struct {
	config   *HeartbeatConfig
	connMgr  *ConnectionManager
	deviceID string
	clientIP string

	// State management
	mutex         sync.RWMutex
	isActive      bool      // Whether heartbeat is active
	isRunning     bool      // Whether the goroutine is running
	missedBeats   int       // Number of consecutive missed heartbeats
	lastHeartbeat time.Time // Time of last heartbeat sent
	lastResponse  time.Time // Time of last response received

	// Channels for coordination
	stopChan     chan struct{}
	activateChan chan bool // To activate/deactivate heartbeat
	responseChan chan bool // For heartbeat responses

	// Callbacks
	onDisconnect func(error)
	onReconnect  func()
}

// NewHeartbeatManager creates a new heartbeat manager
func NewHeartbeatManager(connMgr *ConnectionManager, deviceID, clientIP string, config *HeartbeatConfig) *HeartbeatManager {
	if config == nil {
		config = DefaultHeartbeatConfig()
	}

	return &HeartbeatManager{
		config:       config,
		connMgr:      connMgr,
		deviceID:     deviceID,
		clientIP:     clientIP,
		stopChan:     make(chan struct{}),
		activateChan: make(chan bool, 10),
		responseChan: make(chan bool, 10),
	}
}

// ActivateHeartbeat activates the heartbeat (called when there's active RPC)
func (hm *HeartbeatManager) ActivateHeartbeat() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if !hm.isActive {
		hm.isActive = true
		hm.missedBeats = 0
		hm.lastHeartbeat = time.Now()

		// Start goroutine if not running
		if !hm.isRunning {
			hm.isRunning = true
			go hm.heartbeatLoop()
			go hm.monitorLoop()
		}

		if hm.config.LogLevel {
			log.Printf("[heartbeat] Heartbeat activated for device %s", hm.deviceID)
		}
	}

	// Send activation signal
	select {
	case hm.activateChan <- true:
	default:
	}
}

// DeactivateHeartbeat deactivates the heartbeat (called when no RPC is active)
func (hm *HeartbeatManager) DeactivateHeartbeat() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.isActive {
		hm.isActive = false
		if hm.config.LogLevel {
			log.Printf("[heartbeat] Heartbeat deactivated for device %s", hm.deviceID)
		}
	}

	// Send deactivation signal
	select {
	case hm.activateChan <- false:
	default:
	}
}

// heartbeatLoop sends heartbeats periodically
func (hm *HeartbeatManager) heartbeatLoop() {
	ticker := time.NewTicker(hm.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.stopChan:
			return
		case active := <-hm.activateChan:
			// Update active state
			hm.mutex.Lock()
			hm.isActive = active
			hm.mutex.Unlock()
		case <-ticker.C:
			// Only send heartbeat if active
			hm.mutex.RLock()
			shouldSend := hm.isActive
			hm.mutex.RUnlock()

			if shouldSend {
				hm.sendHeartbeat()
			}
		}
	}
}

// sendHeartbeat sends a heartbeat to the server using separate heartbeat queue
func (hm *HeartbeatManager) sendHeartbeat() {
	conn, err := hm.connMgr.GetConnection()
	if err != nil {
		hm.handleMissedHeartbeat("no connection")
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		hm.handleMissedHeartbeat("failed to create channel")
		return
	}
	defer ch.Close()

	// Declare exclusive reply queue for heartbeat response
	replyQueue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		hm.handleMissedHeartbeat("failed to declare reply queue")
		return
	}

	// Generate unique correlation ID
	corrID := fmt.Sprintf("heartbeat_%d", time.Now().UnixNano())

	// Build heartbeat request (PING)
	ping := map[string]interface{}{
		"type":      "heartbeat_ping",
		"deviceID":  hm.deviceID,
		"clientIP":  hm.clientIP,
		"timestamp": time.Now().Unix(),
		"corrID":    corrID,
	}

	body, _ := json.Marshal(ping)

	// Send PING to separate heartbeat queue
	heartbeatQueueName := fmt.Sprintf("device_%s_heartbeat", hm.deviceID)
	err = ch.PublishWithContext(context.Background(), "", heartbeatQueueName, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       replyQueue.Name,
		Body:          body,
	})
	if err != nil {
		hm.handleMissedHeartbeat("failed to send heartbeat ping")
		return
	}

	// Start consuming from reply queue
	msgs, err := ch.Consume(replyQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		hm.handleMissedHeartbeat("failed to consume heartbeat response")
		return
	}

	// Wait for response or timeout
	select {
	case msg := <-msgs:
		if msg.CorrelationId == corrID {
			hm.handleHeartbeatResponse()
		}
	case <-time.After(hm.config.Timeout):
		hm.handleMissedHeartbeat("timeout waiting for heartbeat pong")
	}
}

// handleHeartbeatResponse processes a successful heartbeat response
func (hm *HeartbeatManager) handleHeartbeatResponse() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	hm.missedBeats = 0
	hm.lastResponse = time.Now()

	select {
	case hm.responseChan <- true:
	default:
	}

	if hm.config.LogLevel {
		log.Printf("[heartbeat] PONG received from server for device %s", hm.deviceID)
	}
}

// handleMissedHeartbeat processes a missed heartbeat
func (hm *HeartbeatManager) handleMissedHeartbeat(reason string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	hm.missedBeats++
	if hm.config.LogLevel {
		log.Printf("[heartbeat] Missed heartbeat #%d: %s (device: %s)",
			hm.missedBeats, reason, hm.deviceID)
	}

	if hm.missedBeats >= hm.config.MaxMissedBeats {
		if hm.config.LogLevel {
			log.Printf("[heartbeat] Connection considered dead after %d missed heartbeats (device: %s)",
				hm.missedBeats, hm.deviceID)
		}
		if hm.onDisconnect != nil {
			hm.onDisconnect(fmt.Errorf("connection dead: %d missed heartbeats", hm.missedBeats))
		}
	}
}

// monitorLoop monitors heartbeat responses
func (hm *HeartbeatManager) monitorLoop() {
	for {
		select {
		case <-hm.stopChan:
			return
		case <-hm.responseChan:
			// Heartbeat response received, reset missed count
			hm.mutex.Lock()
			hm.missedBeats = 0
			hm.mutex.Unlock()
		}
	}
}

// Stop stops the heartbeat manager
func (hm *HeartbeatManager) Stop() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.isRunning {
		hm.isRunning = false
		hm.isActive = false
		close(hm.stopChan)
		if hm.config.LogLevel {
			log.Printf("[heartbeat] Heartbeat manager stopped for device %s", hm.deviceID)
		}
	}
}

// SetCallbacks sets the callbacks for events
func (hm *HeartbeatManager) SetCallbacks(onDisconnect func(error), onReconnect func()) {
	hm.onDisconnect = onDisconnect
	hm.onReconnect = onReconnect
}

// GetStats returns heartbeat statistics
func (hm *HeartbeatManager) GetStats() HeartbeatStats {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	return HeartbeatStats{
		IsActive:      hm.isActive,
		IsRunning:     hm.isRunning,
		MissedBeats:   hm.missedBeats,
		LastHeartbeat: hm.lastHeartbeat,
		LastResponse:  hm.lastResponse,
	}
}

// HeartbeatStats holds heartbeat monitoring statistics
type HeartbeatStats struct {
	IsActive      bool      // Whether heartbeat is active
	IsRunning     bool      // Whether the goroutine is running
	MissedBeats   int       // Number of consecutive missed heartbeats
	LastHeartbeat time.Time // Time of last heartbeat sent
	LastResponse  time.Time // Time of last response received
}
