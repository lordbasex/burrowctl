package server

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ServerHeartbeatConfig holds configuration for server-side heartbeat handling
type ServerHeartbeatConfig struct {
	Enabled         bool          // Whether heartbeat handling is enabled
	ResponseTimeout time.Duration // How long to wait before responding to heartbeat
	CleanupInterval time.Duration // How often to cleanup stale client connections
	MaxClientAge    time.Duration // Maximum age of client connection before cleanup
}

// DefaultServerHeartbeatConfig returns sensible default server heartbeat configuration
func DefaultServerHeartbeatConfig() *ServerHeartbeatConfig {
	return &ServerHeartbeatConfig{
		Enabled:         true,
		ResponseTimeout: 100 * time.Millisecond, // Quick response
		CleanupInterval: 2 * time.Minute,        // Cleanup every 2 minutes
		MaxClientAge:    3 * time.Minute,        // Remove clients older than 3 minutes
	}
}

// ClientHeartbeatInfo tracks client connection state
type ClientHeartbeatInfo struct {
	DeviceID  string    // Device identifier
	ClientIP  string    // Client IP address
	LastPing  time.Time // Last time client sent PING
	LastPong  time.Time // Last time server sent PONG
	IsActive  bool      // Whether connection is considered active
	PingCount int       // Number of PINGs received
	RPCActive bool      // Whether RPC is active for this client
}

// ServerHeartbeatManager handles server-side heartbeat processing with separate queues
type ServerHeartbeatManager struct {
	config   *ServerHeartbeatConfig
	deviceID string

	// Client tracking
	mutex   sync.RWMutex
	clients map[string]*ClientHeartbeatInfo // clientIP -> connection info

	// Cleanup
	stopChan chan struct{}
}

// NewServerHeartbeatManager creates a new server heartbeat manager
func NewServerHeartbeatManager(deviceID string, config *ServerHeartbeatConfig) *ServerHeartbeatManager {
	if config == nil {
		config = DefaultServerHeartbeatConfig()
	}

	return &ServerHeartbeatManager{
		config:   config,
		deviceID: deviceID,
		clients:  make(map[string]*ClientHeartbeatInfo),
		stopChan: make(chan struct{}),
	}
}

// Start begins server heartbeat management
func (shm *ServerHeartbeatManager) Start() {
	if !shm.config.Enabled {
		return
	}

	go shm.cleanupLoop()
	log.Printf("[server-heartbeat] Started heartbeat management for device %s", shm.deviceID)
}

// Stop stops server heartbeat management
func (shm *ServerHeartbeatManager) Stop() {
	close(shm.stopChan)
	log.Printf("[server-heartbeat] Stopped heartbeat management for device %s", shm.deviceID)
}

// HandleHeartbeatPing processes a heartbeat PING request from a client
func (shm *ServerHeartbeatManager) HandleHeartbeatPing(ch *amqp.Channel, msg amqp.Delivery) {
	if !shm.config.Enabled {
		log.Printf("[server-heartbeat] Heartbeat disabled, ignoring ping")
		return
	}

	// Validate message body is not empty
	if len(msg.Body) == 0 {
		log.Printf("[server-heartbeat] Received empty heartbeat message from %s (correlation: %s)",
			msg.ReplyTo, msg.CorrelationId)
		return
	}

	// Log raw message for debugging (truncated to avoid spam)
	bodyStr := string(msg.Body)
	if len(bodyStr) > 200 {
		bodyStr = bodyStr[:200] + "..."
	}
	log.Printf("[server-heartbeat] Processing heartbeat message: %s", bodyStr)

	var ping map[string]interface{}
	if err := json.Unmarshal(msg.Body, &ping); err != nil {
		log.Printf("[server-heartbeat] Failed to parse heartbeat ping: %v (body: %s)", err, string(msg.Body))
		return
	}

	// Validate required fields with detailed logging
	deviceID, ok := ping["deviceID"].(string)
	if !ok {
		log.Printf("[server-heartbeat] Missing or invalid deviceID in heartbeat ping (got: %T)", ping["deviceID"])
		return
	}

	clientIP, ok := ping["clientIP"].(string)
	if !ok {
		log.Printf("[server-heartbeat] Missing or invalid clientIP in heartbeat ping (got: %T)", ping["clientIP"])
		return
	}

	corrID, ok := ping["corrID"].(string)
	if !ok {
		log.Printf("[server-heartbeat] Missing or invalid corrID in heartbeat ping (got: %T)", ping["corrID"])
		return
	}

	// Verify this server handles this device
	if deviceID != shm.deviceID {
		log.Printf("[server-heartbeat] Ignoring heartbeat for device %s (this server handles %s)",
			deviceID, shm.deviceID)
		return
	}

	// Update client connection info
	shm.mutex.Lock()
	client, exists := shm.clients[clientIP]
	if !exists {
		client = &ClientHeartbeatInfo{
			DeviceID: deviceID,
			ClientIP: clientIP,
		}
		shm.clients[clientIP] = client
		log.Printf("[server-heartbeat] New client registered: %s", clientIP)
	}

	client.LastPing = time.Now()
	client.IsActive = true
	client.PingCount++
	shm.mutex.Unlock()

	// Respond with PONG
	shm.sendHeartbeatPong(ch, msg.ReplyTo, corrID, deviceID, clientIP)

	log.Printf("[server-heartbeat] PING received from %s (device: %s, total pings: %d)",
		clientIP, deviceID, client.PingCount)
}

// sendHeartbeatPong sends a heartbeat PONG response to the client
func (shm *ServerHeartbeatManager) sendHeartbeatPong(ch *amqp.Channel, replyTo, corrID, deviceID, clientIP string) {
	// Build heartbeat response (PONG)
	pong := map[string]interface{}{
		"type":      "heartbeat_pong",
		"deviceID":  deviceID,
		"clientIP":  clientIP,
		"timestamp": time.Now().Unix(),
		"corrID":    corrID,
		"serverID":  shm.deviceID, // Server identifier
	}

	body, _ := json.Marshal(pong)

	// Send PONG response
	err := ch.PublishWithContext(context.Background(), "", replyTo, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		Body:          body,
	})

	if err != nil {
		log.Printf("[server-heartbeat] Failed to send PONG to %s: %v", clientIP, err)
	} else {
		log.Printf("[server-heartbeat] PONG sent to %s", clientIP)
	}
}

// cleanupLoop periodically cleans up stale client connections
func (shm *ServerHeartbeatManager) cleanupLoop() {
	ticker := time.NewTicker(shm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-shm.stopChan:
			return
		case <-ticker.C:
			shm.cleanupStaleConnections()
		}
	}
}

// cleanupStaleConnections removes clients that haven't been seen recently
func (shm *ServerHeartbeatManager) cleanupStaleConnections() {
	shm.mutex.Lock()
	defer shm.mutex.Unlock()

	now := time.Now()
	removed := 0

	for clientIP, client := range shm.clients {
		if now.Sub(client.LastPing) > shm.config.MaxClientAge {
			// Log specifically about heartbeat timeout disconnection
			timeoutDuration := now.Sub(client.LastPing)
			log.Printf("[server-heartbeat] ⚠️  CLIENT DISCONNECTED BY TIMEOUT: %s (device: %s)",
				clientIP, client.DeviceID)
			log.Printf("[server-heartbeat] 💔 HEARTBEAT CLOSED: Client %s exceeded timeout (no PING for %v, max allowed: %v)",
				clientIP, timeoutDuration, shm.config.MaxClientAge)
			log.Printf("[server-heartbeat] 📊 Connection stats - Total PINGs received: %d, Last PING: %v",
				client.PingCount, client.LastPing.Format("15:04:05"))

			client.IsActive = false
			removed++

			// Additional log for the original inactive marking
			log.Printf("[server-heartbeat] Client %s marked as inactive (no PING for %v)",
				clientIP, now.Sub(client.LastPing))
		}
	}

	if removed > 0 {
		log.Printf("[server-heartbeat] 🧹 Cleaned up %d inactive clients due to heartbeat timeout", removed)
	}
}

// GetActiveClients returns information about active client connections
func (shm *ServerHeartbeatManager) GetActiveClients() map[string]*ClientHeartbeatInfo {
	shm.mutex.RLock()
	defer shm.mutex.RUnlock()

	result := make(map[string]*ClientHeartbeatInfo)
	for clientIP, client := range shm.clients {
		if client.IsActive {
			result[clientIP] = &ClientHeartbeatInfo{
				DeviceID:  client.DeviceID,
				ClientIP:  client.ClientIP,
				LastPing:  client.LastPing,
				LastPong:  client.LastPong,
				IsActive:  client.IsActive,
				PingCount: client.PingCount,
				RPCActive: client.RPCActive,
			}
		}
	}

	return result
}

// GetStats returns heartbeat statistics
func (shm *ServerHeartbeatManager) GetStats() ServerHeartbeatStats {
	shm.mutex.RLock()
	defer shm.mutex.RUnlock()

	activeClients := 0
	totalPings := 0

	for _, client := range shm.clients {
		if client.IsActive {
			activeClients++
		}
		totalPings += client.PingCount
	}

	return ServerHeartbeatStats{
		DeviceID:      shm.deviceID,
		ActiveClients: activeClients,
		TotalClients:  len(shm.clients),
		TotalPings:    totalPings,
		IsEnabled:     shm.config.Enabled,
	}
}

// ServerHeartbeatStats holds server heartbeat statistics
type ServerHeartbeatStats struct {
	DeviceID      string // Device identifier
	ActiveClients int    // Number of active clients
	TotalClients  int    // Total number of clients tracked
	TotalPings    int    // Total number of PINGs received
	IsEnabled     bool   // Whether heartbeat is enabled
}
