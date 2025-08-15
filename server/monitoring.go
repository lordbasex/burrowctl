package server

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// MonitoringManager handles comprehensive server monitoring and reporting
type MonitoringManager struct {
	handler   *Handler
	config    *ServerConfig
	startTime time.Time
	stopChan  chan struct{}
}

// NewMonitoringManager creates a new monitoring manager
func NewMonitoringManager(handler *Handler, config *ServerConfig) *MonitoringManager {
	return &MonitoringManager{
		handler:   handler,
		config:    config,
		startTime: time.Now(),
		stopChan:  make(chan struct{}),
	}
}

// Start begins comprehensive monitoring
func (mm *MonitoringManager) Start() {
	if !mm.config.MonitoringEnabled {
		return
	}

	go mm.monitoringLoop()
	if mm.config.LogLevel {
		log.Printf("[monitoring] Started comprehensive monitoring with interval: %v", mm.config.MonitoringInterval)
	}
}

// Stop stops the monitoring manager
func (mm *MonitoringManager) Stop() {
	close(mm.stopChan)
	if mm.config.LogLevel {
		log.Printf("[monitoring] Stopped monitoring manager")
	}
}

// monitoringLoop provides detailed monitoring of all server components
func (mm *MonitoringManager) monitoringLoop() {
	ticker := time.NewTicker(mm.config.MonitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.stopChan:
			return
		case <-ticker.C:
			mm.printComprehensiveStats()
		}
	}
}

// printComprehensiveStats prints detailed statistics for all components
func (mm *MonitoringManager) printComprehensiveStats() {
	cacheStats := mm.handler.GetCacheStats()
	validationStats := mm.handler.GetSQLValidationStats()

	// Only show report if there's significant activity
	totalActivity := cacheStats.TotalRequests + validationStats.TotalQueries
	if totalActivity == 0 {
		// Just show a simple status message when idle
		if mm.config.LogLevel {
			fmt.Printf("📊 System Status: Idle (Uptime: %v)\n", time.Since(mm.startTime).Round(time.Second))
		}
		return
	}

	if mm.config.LogLevel {
		fmt.Printf("\n" + strings.Repeat("=", 60) + "\n")
		fmt.Printf("📊 COMPREHENSIVE SYSTEM REPORT - %s\n", time.Now().Format("15:04:05"))
		fmt.Printf(strings.Repeat("=", 60) + "\n")
	}

	// System Overview
	if mm.config.LogLevel {
		fmt.Printf("🏢 System Overview:\n")
		fmt.Printf("  Uptime: %v\n", time.Since(mm.startTime).Round(time.Second))
	}

	// Cache Statistics
	if mm.config.LogLevel {
		fmt.Printf("\n📈 Cache Performance:\n")
		fmt.Printf("  Total Requests: %d\n", cacheStats.TotalRequests)
		fmt.Printf("  Cache Hits: %d\n", cacheStats.Hits)
		fmt.Printf("  Cache Misses: %d\n", cacheStats.Misses)
	}
	if cacheStats.TotalRequests > 0 {
		hitRatio := float64(cacheStats.Hits) / float64(cacheStats.TotalRequests) * 100
		if mm.config.LogLevel {
			fmt.Printf("  Hit Ratio: %.2f%%\n", hitRatio)
		}
	}
	if mm.config.LogLevel {
		fmt.Printf("  Current Size: %d entries\n", cacheStats.CurrentSize)
		fmt.Printf("  Evictions: %d\n", cacheStats.Evictions)
		fmt.Printf("  Expirations: %d\n", cacheStats.Expirations)
	}

	// Validation Statistics
	if mm.config.LogLevel {
		fmt.Printf("\n🛡️ Security & Validation:\n")
		fmt.Printf("  Total Queries: %d\n", validationStats.TotalQueries)
		fmt.Printf("  Valid Queries: %d\n", validationStats.ValidQueries)
		fmt.Printf("  Blocked Queries: %d\n", validationStats.BlockedQueries)
		fmt.Printf("  Injection Attempts: %d\n", validationStats.InjectionAttempts)
		fmt.Printf("  Command Violations: %d\n", validationStats.CommandViolations)
		fmt.Printf("  Structure Violations: %d\n", validationStats.StructureViolations)
	}

	if validationStats.TotalQueries > 0 {
		blockRate := float64(validationStats.BlockedQueries) / float64(validationStats.TotalQueries) * 100
		injectionRate := float64(validationStats.InjectionAttempts) / float64(validationStats.TotalQueries) * 100

		if mm.config.LogLevel {
			fmt.Printf("  Block Rate: %.2f%%\n", blockRate)
			fmt.Printf("  Injection Rate: %.2f%%\n", injectionRate)
			fmt.Printf("  Security Level: %s\n", mm.getSecurityLevel(blockRate/100, injectionRate/100))
		}

		// Security alerts
		if injectionRate > 5 {
			if mm.config.LogLevel {
				fmt.Printf("  🚨 HIGH INJECTION RATE DETECTED!\n")
			}
		}
		if blockRate > 30 {
			if mm.config.LogLevel {
				fmt.Printf("  ⚠️  High block rate - review policies\n")
			}
		}
	}

	// Performance Summary
	if mm.config.LogLevel {
		fmt.Printf("\n⚡ Performance Summary:\n")
	}
	if cacheStats.TotalRequests > 0 && validationStats.TotalQueries > 0 {
		cacheEfficiency := float64(cacheStats.Hits) / float64(cacheStats.TotalRequests) * 100
		validationEfficiency := float64(validationStats.ValidQueries) / float64(validationStats.TotalQueries) * 100

		if mm.config.LogLevel {
			fmt.Printf("  Cache Efficiency: %.2f%%\n", cacheEfficiency)
			fmt.Printf("  Validation Efficiency: %.2f%%\n", validationEfficiency)
		}

		// Performance recommendations
		if cacheEfficiency < 50 {
			if mm.config.LogLevel {
				fmt.Printf("  💡 Consider increasing cache size or TTL\n")
			}
		}
		if validationEfficiency < 80 {
			if mm.config.LogLevel {
				fmt.Printf("  💡 Review validation policies - high block rate\n")
			}
		}
	}

	if mm.config.LogLevel {
		fmt.Printf(strings.Repeat("=", 60) + "\n")
	}
}

// getSecurityLevel determines the current security threat level
func (mm *MonitoringManager) getSecurityLevel(blockRate, injectionRate float64) string {
	if injectionRate > 0.1 {
		return "HIGH"
	} else if blockRate > 0.2 {
		return "MEDIUM"
	} else if injectionRate > 0.01 {
		return "ELEVATED"
	} else {
		return "LOW"
	}
}

// DisplayConfiguration shows the complete server configuration
func (mm *MonitoringManager) DisplayConfiguration() {
	if mm.config.LogLevel {
		fmt.Printf("🏢 Full-Featured Enterprise Server Configuration\n")
		fmt.Printf("===============================================\n")
		fmt.Printf("Device ID: %s\n", mm.config.DeviceID)
		fmt.Printf("AMQP URL: %s\n", mm.config.AMQPURL)
		fmt.Printf("MySQL DSN: %s\n", mm.config.MySQLDSN)

		fmt.Printf("\n📊 Cache Configuration:\n")
		fmt.Printf("  Enabled: %v\n", mm.config.CacheEnabled)
		fmt.Printf("  Max Size: %d queries\n", mm.config.CacheSize)
		fmt.Printf("  TTL: %v\n", mm.config.CacheTTL)
		fmt.Printf("  Cleanup Interval: %v\n", mm.config.CacheCleanup)

		fmt.Printf("\n🔒 SQL Validation Configuration:\n")
		fmt.Printf("  Enabled: %v\n", mm.config.ValidationEnabled)
		fmt.Printf("  Strict Mode: %v\n", mm.config.StrictMode)
		fmt.Printf("  Allow DDL: %v\n", mm.config.AllowDDL)
		fmt.Printf("  Allow DML: %v\n", mm.config.AllowDML)
		fmt.Printf("  Allow Stored Procedures: %v\n", mm.config.AllowStoredProcs)
		fmt.Printf("  Max Query Length: %d\n", mm.config.MaxQueryLength)

		fmt.Printf("\n⚙️ Performance Configuration:\n")
		fmt.Printf("  Workers: %d\n", mm.config.Workers)
		fmt.Printf("  Queue Size: %d\n", mm.config.QueueSize)
		fmt.Printf("  Rate Limit: %d req/s\n", mm.config.RateLimit)
		fmt.Printf("  Burst Size: %d\n", mm.config.BurstSize)

		fmt.Printf("\n🗄️ Database Configuration:\n")
		fmt.Printf("  Max Idle Connections: %d\n", mm.config.PoolIdle)
		fmt.Printf("  Max Open Connections: %d\n", mm.config.PoolOpen)
		fmt.Printf("  Connection Lifetime: %v\n", mm.config.ConnLifetime)

		fmt.Printf("\n🔄 Heartbeat Configuration:\n")
		fmt.Printf("  Enabled: %v\n", mm.config.HeartbeatEnabled)
		fmt.Printf("  Interval: %v\n", mm.config.HeartbeatInterval)
		fmt.Printf("  Timeout: %v\n", mm.config.HeartbeatTimeout)
		fmt.Printf("  Max Missed: %d\n", mm.config.HeartbeatMaxMissed)
		fmt.Printf("  Cleanup Interval: %v\n", mm.config.HeartbeatCleanup)
		fmt.Printf("  Max Client Age: %v\n", mm.config.HeartbeatMaxClientAge)

		fmt.Printf("\n🔄 Starting comprehensive monitoring...\n")
	}
}

// RegisterMonitoringFunctions registers comprehensive monitoring functions
func (mm *MonitoringManager) RegisterMonitoringFunctions() {
	// Cache statistics
	mm.handler.RegisterFunction("getCacheStats", func() map[string]interface{} {
		stats := mm.handler.GetCacheStats()
		hitRatio := float64(0)
		if stats.TotalRequests > 0 {
			hitRatio = float64(stats.Hits) / float64(stats.TotalRequests)
		}
		return map[string]interface{}{
			"hits":           stats.Hits,
			"misses":         stats.Misses,
			"hit_ratio":      hitRatio,
			"total_requests": stats.TotalRequests,
			"current_size":   stats.CurrentSize,
			"evictions":      stats.Evictions,
			"expirations":    stats.Expirations,
			"last_cleanup":   stats.LastCleanup.Format(time.RFC3339),
		}
	})

	// Validation statistics
	mm.handler.RegisterFunction("getValidationStats", func() map[string]interface{} {
		stats := mm.handler.GetSQLValidationStats()
		blockRate := float64(0)
		injectionRate := float64(0)
		if stats.TotalQueries > 0 {
			blockRate = float64(stats.BlockedQueries) / float64(stats.TotalQueries)
			injectionRate = float64(stats.InjectionAttempts) / float64(stats.TotalQueries)
		}
		return map[string]interface{}{
			"total_queries":        stats.TotalQueries,
			"valid_queries":        stats.ValidQueries,
			"blocked_queries":      stats.BlockedQueries,
			"injection_attempts":   stats.InjectionAttempts,
			"command_violations":   stats.CommandViolations,
			"structure_violations": stats.StructureViolations,
			"block_rate":           blockRate,
			"injection_rate":       injectionRate,
			"security_level":       mm.getSecurityLevel(blockRate, injectionRate),
		}
	})

	// Overall system status
	mm.handler.RegisterFunction("getSystemStatus", func() map[string]interface{} {
		cacheStats := mm.handler.GetCacheStats()
		validationStats := mm.handler.GetSQLValidationStats()

		cacheHitRatio := float64(0)
		if cacheStats.TotalRequests > 0 {
			cacheHitRatio = float64(cacheStats.Hits) / float64(cacheStats.TotalRequests)
		}

		blockRate := float64(0)
		if validationStats.TotalQueries > 0 {
			blockRate = float64(validationStats.BlockedQueries) / float64(validationStats.TotalQueries)
		}

		return map[string]interface{}{
			"status":             "healthy",
			"uptime":             time.Since(mm.startTime).String(),
			"cache_hit_ratio":    cacheHitRatio,
			"cache_size":         cacheStats.CurrentSize,
			"validation_enabled": validationStats.TotalQueries > 0,
			"security_level":     mm.getSecurityLevel(blockRate, 0),
			"total_queries":      validationStats.TotalQueries,
			"blocked_queries":    validationStats.BlockedQueries,
			"injection_attempts": validationStats.InjectionAttempts,
		}
	})

	// Performance metrics
	mm.handler.RegisterFunction("getPerformanceMetrics", func() map[string]interface{} {
		cacheStats := mm.handler.GetCacheStats()
		validationStats := mm.handler.GetSQLValidationStats()

		return map[string]interface{}{
			"cache_performance": map[string]interface{}{
				"hit_ratio":    float64(cacheStats.Hits) / float64(cacheStats.TotalRequests),
				"evictions":    cacheStats.Evictions,
				"current_size": cacheStats.CurrentSize,
			},
			"validation_performance": map[string]interface{}{
				"total_queries":   validationStats.TotalQueries,
				"blocked_queries": validationStats.BlockedQueries,
				"processing_rate": float64(validationStats.ValidQueries) / float64(validationStats.TotalQueries),
			},
		}
	})

	// Clear all caches and stats
	mm.handler.RegisterFunction("clearAllCaches", func() string {
		mm.handler.ClearCache()
		return "All caches cleared successfully"
	})
}
