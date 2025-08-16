package server

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
)

// SQLValidator provides comprehensive SQL query validation for security and policy enforcement.
// It implements multiple validation layers including command whitelisting, injection detection,
// and structural analysis to ensure queries are safe and compliant with configured policies.
//
// Features:
// - Command whitelist/blacklist enforcement
// - SQL injection pattern detection
// - Structural query validation
// - Configurable validation rules
// - Detailed validation error reporting
// - Performance optimized with compiled regex patterns
type SQLValidator struct {
	config           SQLValidationConfig // Validation configuration
	injectionRegexes []*regexp.Regexp    // Compiled injection detection patterns
	mutex            sync.RWMutex        // Thread-safe access to validator state
	stats            ValidationStats     // Validation statistics
}

// SQLValidationConfig defines the validation rules and policies.
type SQLValidationConfig struct {
	Enabled               bool     // Whether validation is enabled
	AllowedCommands       []string // Whitelist of allowed SQL commands (e.g., SELECT, INSERT)
	BlockedCommands       []string // Blacklist of forbidden SQL commands (e.g., DROP, ALTER)
	AllowDDL              bool     // Allow Data Definition Language (CREATE, ALTER, DROP)
	AllowDML              bool     // Allow Data Manipulation Language (INSERT, UPDATE, DELETE)
	AllowDQL              bool     // Allow Data Query Language (SELECT)
	AllowStoredProcedures bool     // Allow stored procedure calls
	MaxQueryLength        int      // Maximum allowed query length
	StrictMode            bool     // Enable strict validation (more restrictive)
	LogViolations         bool     // Log validation violations
	LogLevel              bool     // Whether to enable detailed logging
}

// ValidationStats tracks validation performance and security metrics.
type ValidationStats struct {
	TotalQueries        int64 // Total queries validated
	ValidQueries        int64 // Queries that passed validation
	BlockedQueries      int64 // Queries blocked by validation
	InjectionAttempts   int64 // Detected SQL injection attempts
	CommandViolations   int64 // Command policy violations
	StructureViolations int64 // Structure policy violations
	mutex               sync.RWMutex
}

// ValidationResult contains the result of SQL validation.
type ValidationResult struct {
	Valid           bool      // Whether the query is valid
	Errors          []string  // List of validation errors
	Warnings        []string  // List of validation warnings
	NormalizedQuery string    // Normalized version of the query
	DetectedCommand string    // Primary SQL command detected
	Risk            RiskLevel // Assessed risk level
}

// RiskLevel represents the security risk level of a query.
type RiskLevel int

const (
	RiskLow RiskLevel = iota
	RiskMedium
	RiskHigh
	RiskCritical
)

// String returns the string representation of a risk level.
func (r RiskLevel) String() string {
	switch r {
	case RiskLow:
		return "low"
	case RiskMedium:
		return "medium"
	case RiskHigh:
		return "high"
	case RiskCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// DefaultSQLValidationConfig returns a secure default configuration.
func DefaultSQLValidationConfig() SQLValidationConfig {
	return SQLValidationConfig{
		Enabled:               true,
		AllowedCommands:       []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		BlockedCommands:       []string{"DROP", "TRUNCATE", "ALTER", "CREATE USER", "GRANT", "REVOKE"},
		AllowDDL:              false, // Disable DDL by default for security
		AllowDML:              true,  // Allow basic data manipulation
		AllowDQL:              true,  // Allow data queries
		AllowStoredProcedures: false, // Disable stored procedures by default
		MaxQueryLength:        10000, // 10KB max query size
		StrictMode:            false, // Balanced security/usability
		LogViolations:         true,  // Log security violations
		LogLevel:              false, // Default: minimal logging
	}
}

// NewSQLValidator creates a new SQL validator with the specified configuration.
func NewSQLValidator(config SQLValidationConfig) *SQLValidator {
	validator := &SQLValidator{
		config: config,
		stats:  ValidationStats{},
	}

	// Compile injection detection patterns
	validator.compileInjectionPatterns()

	if config.LogLevel {
		log.Printf("[server] SQL validator initialized: enabled=%v, strict=%v",
			config.Enabled, config.StrictMode)
	}

	return validator
}

// compileInjectionPatterns compiles regex patterns for SQL injection detection.
func (v *SQLValidator) compileInjectionPatterns() {
	// Common SQL injection patterns
	patterns := []string{
		// Union-based injection
		`(?i)\bunion\s+(?:all\s+)?select\b`,

		// Comment-based injection
		`(?i)(/\*.*?\*/|--.*?$|#.*?$)`,

		// Boolean-based injection
		`(?i)\b(and|or)\s+\d+\s*[=<>]\s*\d+\b`,
		`(?i)\b(and|or)\s+['"][^'"]*['"]\s*[=<>]\s*['"][^'"]*['"]`,

		// Time-based injection
		`(?i)\b(sleep|benchmark|pg_sleep|waitfor\s+delay)\s*\(`,

		// Stacked queries
		`(?i);\s*(select|insert|update|delete|drop|create|alter)\b`,

		// Function-based injection
		`(?i)\b(load_file|into\s+outfile|into\s+dumpfile)\b`,
		`(?i)\b(exec|execute|sp_executesql)\s*\(`,

		// Information schema attacks
		`(?i)\binformation_schema\b`,
		`(?i)\bmysql\.user\b`,
		`(?i)\bsys\.databases\b`,

		// Hexadecimal/char encoding
		`(?i)\b(0x[0-9a-f]+|char\s*\(\s*\d+\s*\))\b`,

		// Conditional logic injection
		`(?i)\bcase\s+when\b.*?\bthen\b`,
		`(?i)\bif\s*\(\s*[^)]*[=<>][^)]*\s*,`,
	}

	v.injectionRegexes = make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		if regex, err := regexp.Compile(pattern); err != nil {
			if v.config.LogLevel {
				log.Printf("[server] Failed to compile injection pattern: %s, error: %v", pattern, err)
			}
		} else {
			v.injectionRegexes = append(v.injectionRegexes, regex)
		}
	}

	if v.config.LogLevel {
		log.Printf("[server] Compiled %d SQL injection detection patterns", len(v.injectionRegexes))
	}
}

// ValidateQuery performs comprehensive validation of a SQL query.
func (v *SQLValidator) ValidateQuery(query string, params []interface{}) ValidationResult {
	v.incrementTotalQueries()

	// Skip validation if disabled
	if !v.config.Enabled {
		return ValidationResult{
			Valid:           true,
			NormalizedQuery: query,
			DetectedCommand: v.detectCommand(query),
			Risk:            RiskLow,
		}
	}

	result := ValidationResult{
		Valid:           true,
		Errors:          []string{},
		Warnings:        []string{},
		NormalizedQuery: v.normalizeQuery(query),
		DetectedCommand: v.detectCommand(query),
		Risk:            RiskLow,
	}

	// 1. Basic length validation
	if len(query) > v.config.MaxQueryLength {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Query exceeds maximum length of %d characters", v.config.MaxQueryLength))
		result.Risk = RiskMedium
	}

	// 2. Empty query validation
	if strings.TrimSpace(query) == "" {
		result.Valid = false
		result.Errors = append(result.Errors, "Empty query not allowed")
		return result
	}

	// 3. Command validation
	if !v.validateCommand(result.DetectedCommand) {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Command '%s' is not allowed by current policy", result.DetectedCommand))
		v.incrementCommandViolations()
		result.Risk = RiskHigh
	}

	// 4. SQL injection detection
	if injectionDetected, injectionType := v.detectSQLInjection(query); injectionDetected {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Potential SQL injection detected: %s", injectionType))
		v.incrementInjectionAttempts()
		result.Risk = RiskCritical
	}

	// 5. Structural validation
	if structureErrors := v.validateStructure(query); len(structureErrors) > 0 {
		if v.config.StrictMode {
			result.Valid = false
			result.Errors = append(result.Errors, structureErrors...)
		} else {
			result.Warnings = append(result.Warnings, structureErrors...)
		}
		v.incrementStructureViolations()
		if result.Risk < RiskMedium {
			result.Risk = RiskMedium
		}
	}

	// 6. Parameter validation
	if paramErrors := v.validateParameters(params); len(paramErrors) > 0 {
		result.Warnings = append(result.Warnings, paramErrors...)
	}

	// Update statistics
	if result.Valid {
		v.incrementValidQueries()
	} else {
		v.incrementBlockedQueries()

		// Log violations if enabled
		if v.config.LogViolations && v.config.LogLevel {
			log.Printf("[server] SQL validation violation: query=%s, errors=%v, risk=%s",
				v.truncateForLog(query), result.Errors, result.Risk)
		}
	}

	return result
}

// detectCommand extracts the primary SQL command from a query.
func (v *SQLValidator) detectCommand(query string) string {
	normalized := strings.TrimSpace(strings.ToUpper(query))

	// Remove leading comments and whitespace
	normalized = regexp.MustCompile(`^(/\*.*?\*/|\s|--.*?\n)*`).ReplaceAllString(normalized, "")

	// Extract first word as command
	words := strings.Fields(normalized)
	if len(words) == 0 {
		return "UNKNOWN"
	}

	return words[0]
}

// validateCommand checks if a command is allowed by the current policy.
func (v *SQLValidator) validateCommand(command string) bool {
	command = strings.ToUpper(command)

	// Check blacklist first
	for _, blocked := range v.config.BlockedCommands {
		if strings.ToUpper(blocked) == command {
			return false
		}
	}

	// Check whitelist if specified
	if len(v.config.AllowedCommands) > 0 {
		for _, allowed := range v.config.AllowedCommands {
			if strings.ToUpper(allowed) == command {
				return true
			}
		}
		return false // Not in whitelist
	}

	// Check by category if no explicit whitelist
	switch command {
	case "SELECT", "SHOW", "DESCRIBE", "EXPLAIN":
		return v.config.AllowDQL

	case "INSERT", "UPDATE", "DELETE":
		return v.config.AllowDML

	case "CREATE", "ALTER", "DROP", "TRUNCATE":
		return v.config.AllowDDL

	case "CALL", "EXEC", "EXECUTE":
		return v.config.AllowStoredProcedures

	default:
		// Unknown commands are blocked in strict mode, allowed otherwise
		return !v.config.StrictMode
	}
}

// detectSQLInjection scans query for SQL injection patterns.
func (v *SQLValidator) detectSQLInjection(query string) (bool, string) {
	for i, regex := range v.injectionRegexes {
		if regex.MatchString(query) {
			return true, fmt.Sprintf("Pattern %d matched", i+1)
		}
	}
	return false, ""
}

// validateStructure performs structural validation of the query.
func (v *SQLValidator) validateStructure(query string) []string {
	var errors []string

	// Check for balanced parentheses
	if !v.hasBalancedParentheses(query) {
		errors = append(errors, "Unbalanced parentheses detected")
	}

	// Check for balanced quotes
	if !v.hasBalancedQuotes(query) {
		errors = append(errors, "Unbalanced quotes detected")
	}

	// Check for suspicious patterns in strict mode
	if v.config.StrictMode {
		if strings.Contains(strings.ToLower(query), "/*") && !strings.Contains(strings.ToLower(query), "*/") {
			errors = append(errors, "Unclosed comment block")
		}

		if strings.Count(query, ";") > 1 {
			errors = append(errors, "Multiple statements not allowed in strict mode")
		}
	}

	return errors
}

// validateParameters validates query parameters for suspicious content.
func (v *SQLValidator) validateParameters(params []interface{}) []string {
	var warnings []string

	for i, param := range params {
		if str, ok := param.(string); ok {
			// Check for SQL keywords in string parameters
			if v.containsSQLKeywords(str) {
				warnings = append(warnings, fmt.Sprintf("Parameter %d contains SQL keywords", i+1))
			}

			// Check for suspicious patterns
			if injected, _ := v.detectSQLInjection(str); injected {
				warnings = append(warnings, fmt.Sprintf("Parameter %d contains suspicious patterns", i+1))
			}
		}
	}

	return warnings
}

// Helper functions for validation
func (v *SQLValidator) hasBalancedParentheses(query string) bool {
	count := 0
	for _, char := range query {
		switch char {
		case '(':
			count++
		case ')':
			count--
			if count < 0 {
				return false
			}
		}
	}
	return count == 0
}

func (v *SQLValidator) hasBalancedQuotes(query string) bool {
	singleQuotes := strings.Count(query, "'") - strings.Count(query, "\\'")
	doubleQuotes := strings.Count(query, "\"") - strings.Count(query, "\\\"")
	return singleQuotes%2 == 0 && doubleQuotes%2 == 0
}

func (v *SQLValidator) containsSQLKeywords(str string) bool {
	keywords := []string{"select", "insert", "update", "delete", "drop", "union", "or", "and"}
	lower := strings.ToLower(str)
	for _, keyword := range keywords {
		if strings.Contains(lower, keyword) {
			return true
		}
	}
	return false
}

func (v *SQLValidator) normalizeQuery(query string) string {
	// Basic normalization: trim whitespace and normalize case for keywords
	normalized := strings.TrimSpace(query)
	// In a production system, you might want more sophisticated normalization
	return normalized
}

func (v *SQLValidator) truncateForLog(query string) string {
	if len(query) <= 100 {
		return query
	}
	return query[:100] + "..."
}

// Statistics methods
func (v *SQLValidator) incrementTotalQueries() {
	v.stats.mutex.Lock()
	v.stats.TotalQueries++
	v.stats.mutex.Unlock()
}

func (v *SQLValidator) incrementValidQueries() {
	v.stats.mutex.Lock()
	v.stats.ValidQueries++
	v.stats.mutex.Unlock()
}

func (v *SQLValidator) incrementBlockedQueries() {
	v.stats.mutex.Lock()
	v.stats.BlockedQueries++
	v.stats.mutex.Unlock()
}

func (v *SQLValidator) incrementInjectionAttempts() {
	v.stats.mutex.Lock()
	v.stats.InjectionAttempts++
	v.stats.mutex.Unlock()
}

func (v *SQLValidator) incrementCommandViolations() {
	v.stats.mutex.Lock()
	v.stats.CommandViolations++
	v.stats.mutex.Unlock()
}

func (v *SQLValidator) incrementStructureViolations() {
	v.stats.mutex.Lock()
	v.stats.StructureViolations++
	v.stats.mutex.Unlock()
}

// GetStats returns current validation statistics.
func (v *SQLValidator) GetStats() ValidationStats {
	v.stats.mutex.RLock()
	defer v.stats.mutex.RUnlock()

	// Return a copy of the stats without the mutex
	return ValidationStats{
		TotalQueries:        v.stats.TotalQueries,
		ValidQueries:        v.stats.ValidQueries,
		BlockedQueries:      v.stats.BlockedQueries,
		InjectionAttempts:   v.stats.InjectionAttempts,
		CommandViolations:   v.stats.CommandViolations,
		StructureViolations: v.stats.StructureViolations,
		// Don't copy the mutex
	}
}

// UpdateConfig updates the validator configuration.
func (v *SQLValidator) UpdateConfig(config SQLValidationConfig) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.config = config
	v.compileInjectionPatterns() // Recompile patterns if needed

	if v.config.LogLevel {
		log.Printf("[server] SQL validator configuration updated")
	}
}
