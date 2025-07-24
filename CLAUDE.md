# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**burrowctl** is a Go library and service that provides a RabbitMQ-based bridge for remote SQL execution, custom functions, and system command execution on devices behind NAT or firewalls. It enables secure remote database access and device control without requiring direct connections.

## Development Commands

Use the comprehensive Makefile for all development tasks:

### Core Commands
- `make help` - Show all available commands
- `make build` - Build project and all examples
- `make test` - Run all tests
- `make test-coverage` - Run tests with coverage
- `make clean` - Clean build artifacts
- `make install` - Install dependencies with `go mod tidy`

### Development Environment
- `make docker-up` - Start development environment (RabbitMQ + MariaDB)
- `make docker-down` - Stop development environment
- `make docker-logs` - Show Docker environment logs
- `make fmt` - Format code
- `make lint` - Run linter
- `make vet` - Run go vet

### Example Commands
- `make run-server-example` - Run basic server example
- `make run-server-advanced` - Run enterprise server with custom configuration
- `make run-server-full` - Run server with all enterprise features
- `make run-sql-example` - Run SQL client example
- `make run-function-example` - Run function client example
- `make run-command-example` - Run command client example
- `make run-extended-client-example` - Run extended client with BurrowClient
- `make run-transaction-example` - Run transaction example
- `make run-cache-example` - Run query cache example
- `make run-validation-example` - Run SQL validation example
- `make list-examples` - List all available examples

### Release Commands
- `make tag VERSION=vX.Y.Z` - Create version tag
- `make release` - Create tag and push
- `make quick-release` - Auto-version release

## Architecture Overview

The project follows **clean architecture** with clear separation of concerns:

### Core Components
1. **Server Core** (`/server/server.go`): Clean infrastructure handling AMQP, MySQL, and HTTP
2. **Go Client** (`/client/`): Native `database/sql` driver implementation  
3. **Node.js Client** (`/client-nodejs/`): TypeScript client with async API
4. **Examples** (`/examples/`): Function registration and usage patterns

### Execution Flow
```
Client (Go/Node.js) → RabbitMQ (AMQP 0-9-1) → Server → Database/System/Functions
```

### Three Execution Types
- **SQL Queries**: Direct database access with parameter binding
- **Custom Functions**: Extensible function system with 16+ built-in functions
- **System Commands**: Execute OS commands with controlled access

## Key Directories

- `/server/` - Core server library (clean, no hardcoded functions)
- `/client/` - Go client implementing `database/sql` driver interface
- `/client-nodejs/` - TypeScript client with type definitions
- `/examples/server/` - Server setup with function registration patterns
- `/examples/client/` - Client usage examples for SQL, functions, and commands

## Development Patterns

### Clean Architecture
- Core server contains NO hardcoded functions
- Functions are registered dynamically via `RegisterFunction()` and `RegisterFunctions()`
- Examples demonstrate proper function registration patterns

### Connection Management
Two modes available:
- **"open"**: Connection pooling (default, better performance)
- **"close"**: Per-query connections (safer, slower)

### DSN Format
Universal DSN format across all clients:
```
deviceID=<device-id>&amqp_uri=<rabbitmq-url>&timeout=<timeout>&debug=<boolean>&reconnect_enabled=<boolean>&reconnect_max_attempts=<int>&prepared_statements=<boolean>
```

## Technology Stack

- **Go 1.22+** with MySQL driver and RabbitMQ client
- **Node.js 16+** with TypeScript support
- **RabbitMQ** for message queuing
- **MySQL/MariaDB** for database operations
- **Docker** for development environment

## Development Environment

The project includes complete Docker Compose setup:
- RabbitMQ with management UI at `localhost:15672`
- MariaDB with automatic initialization
- Health checks and proper startup sequencing

## 🚀 Enterprise Features (NEW)

burrowctl now includes enterprise-grade features for production environments:

### Client Features
- **🔄 Automatic Reconnection**: Intelligent connection recovery with exponential backoff
- **📝 Prepared Statements**: Performance optimization and SQL injection protection
- **⚙️ Advanced Configuration**: Customizable timeouts, debug modes, and connection parameters

### Server Features  
- **🏗️ Worker Pool**: Configurable concurrent message processing (10-50+ workers)
- **🛡️ Rate Limiting**: Per-client IP protection using token bucket algorithm
- **💾 Connection Pooling**: Optimized database connection management
- **📊 Performance Tuning**: Granular control over all performance parameters

### Examples
- `examples/client/advanced/` - Advanced client with all features demonstrated
- `examples/server/advanced/` - Enterprise server with full configuration options
- `examples/ADVANCED_FEATURES.md` - Complete documentation and configuration guide

### Configuration Examples
```bash
# High-performance server
cd examples/server/advanced && go run main.go \
  -workers=20 -queue-size=500 -rate-limit=50 -pool-open=50

# Advanced client with reconnection
cd examples/client/advanced && go run advanced-main.go \
  -timeout=30s -prepared -debug=true

# Stress testing
cd examples/client/advanced && go run advanced-main.go \
  -stress -concurrent=10 -requests=100
```

All new features are **100% backward compatible** - existing code benefits automatically.

## 📋 Examples

### Client Examples
- `examples/client/sql-example/` - Basic SQL query execution
- `examples/client/function-example/` - Remote function calls  
- `examples/client/command-example/` - System command execution
- `examples/client/advanced/` - **NEW**: Advanced features demo (reconnection, prepared statements, stress testing)

### Server Examples
- `examples/server/basic/` - Basic server with function registry and Docker setup
- `examples/server/advanced/` - **NEW**: Enterprise server with configurable worker pool, rate limiting, and performance tuning

## Version Management

The Makefile includes automatic versioning based on git commits, updating `version.txt` automatically. Current version is tracked in `version.txt`.

### Testing Commands
- `make test` - Run all tests for project and examples
- `make test-coverage` - Run tests with coverage reporting
- `make test-examples` - Run tests specifically for examples

### Advanced Server Configuration Examples
```bash
# High-performance server with rate limiting
make run-server-advanced

# Server with query caching enabled
make run-server-cache

# Server with strict SQL validation
make run-server-validation

# Full enterprise server with all features
make run-server-full
```

## Key Architecture Files

When working with this codebase, these are the most important files to understand:

### Core Server Files
- `/server/server.go` - Main server implementation and message handling
- `/server/config.go` - Configuration management and validation
- `/server/worker_pool.go` - Concurrent message processing system
- `/server/rate_limiter.go` - Rate limiting implementation using token bucket
- `/server/query_cache.go` - Query result caching system
- `/server/sql_validator.go` - SQL security validation and policy enforcement

### Core Client Files
- `/client/driver.go` - SQL driver interface implementation
- `/client/conn.go` - Connection management and protocol handling
- `/client/burrow_client.go` - Extended client interface for functions/commands
- `/client/reconnect.go` - Automatic reconnection with exponential backoff

### Module Dependencies
- `github.com/go-sql-driver/mysql v1.7.1` - MySQL database driver
- `github.com/rabbitmq/amqp091-go v1.10.0` - RabbitMQ AMQP client