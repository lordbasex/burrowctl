# Makefile para burrowctl
# Versión por defecto
# ---------------- Gestión automática de versión ----------------
# Obtiene el hash corto de git; si falla (por ejemplo, sin repo), muestra 'development'
git_hash := $(shell git rev-parse --short HEAD 2>/dev/null || echo 'development')

# Archivo donde se persiste la versión
version_file := version.txt

# Versión inicial por defecto si el archivo no existe
initial_version := 1.0.0

# Crear version.txt si no existe
ifeq ($(wildcard $(version_file)),)
$(shell echo "initial_version: $(initial_version)" > $(version_file))
$(shell echo "git_hash: $(git_hash)" >> $(version_file))
endif

# Leer última versión y hash almacenados
last_version := $(shell awk -F': ' '/initial_version:/ {print $$2}' $(version_file) | xargs)
last_git_hash := $(shell awk -F': ' '/git_hash:/ {print $$2}' $(version_file) | xargs)

# Calcular la versión a usar
ifeq ($(strip $(git_hash)), $(strip $(last_git_hash)))
VERSION := $(last_version)
else
# Incrementar el número de versión (patch → minor → major)
VERSION := $(shell \
  major=$$(echo $(last_version) | awk -F. '{print $$1}'); \
  minor=$$(echo $(last_version) | awk -F. '{print $$2}'); \
  patch=$$(echo $(last_version) | awk -F. '{print $$3}'); \
  if [ $$patch -eq 9 ]; then \
    if [ $$minor -eq 9 ]; then \
      echo "$$((major + 1)).0.0"; \
    else \
      echo "$$major.$$((minor + 1)).0"; \
    fi; \
  else \
    echo "$$major.$$minor.$$((patch + 1))"; \
  fi)
# Actualizar version.txt con la nueva versión y hash
$(shell echo "initial_version: $(VERSION)" > $(version_file))
$(shell echo "git_hash: $(git_hash)" >> $(version_file))
endif

# Mensajes informativos
$(info git_hash: $(git_hash))
$(info last_version: $(last_version))
$(info last_git_hash: $(last_git_hash))
$(info next VERSION: $(VERSION))

# Configuración del proyecto
PROJECT_NAME = burrowctl
MODULE_NAME = github.com/lordbasex/burrowctl
GO_VERSION = 1.22.0

# Colores para output
RED = \033[0;31m
GREEN = \033[0;32m
YELLOW = \033[0;33m
BLUE = \033[0;34m
NC = \033[0m # No Color

# Ayuda por defecto
.PHONY: help
help: ## Muestra esta ayuda
	@echo "$(BLUE)$(PROJECT_NAME) - Makefile$(NC)"
	@echo "Uso: make [target]"
	@echo ""
	@echo "Targets disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'

# Tareas de desarrollo
.PHONY: install
install: ## Instala dependencias
	@echo "$(GREEN)📦 Instalando dependencias...$(NC)"
	go mod download
	go mod tidy

.PHONY: build
build: build-examples ## Compila el proyecto y ejemplos
	@echo "$(GREEN)🔨 Compilando proyecto...$(NC)"
	go build -v ./...

.PHONY: lint
lint: ## Ejecuta el linter
	@echo "$(GREEN)🔍 Ejecutando linter...$(NC)"
	@which golangci-lint > /dev/null || (echo "$(RED)golangci-lint no está instalado. Instálalo con: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest$(NC)" && exit 1)
	golangci-lint run

.PHONY: fmt
fmt: ## Formatea el código
	@echo "$(GREEN)✨ Formateando código...$(NC)"
	go fmt ./...

# Tareas de limpieza
.PHONY: clean
clean: clean-examples ## Limpia archivos generados
	@echo "$(GREEN)🧹 Limpiando archivos generados...$(NC)"
	go clean
	rm -f coverage.out coverage.html

# Tareas de release
.PHONY: check-git-clean
check-git-clean: ## Verifica que el git esté limpio
	@echo "$(GREEN)🔍 Verificando estado de git...$(NC)"
	@UNCOMMITTED=$$(git status --porcelain | grep -v "^ M version.txt$$" | grep -v "^M  version.txt$$"); \
	if [ -n "$$UNCOMMITTED" ]; then \
		echo "$(YELLOW)⚠️  Advertencia: Hay cambios sin commit$(NC)"; \
		git status --short | grep -v "version.txt"; \
	else \
		echo "$(GREEN)✅ Git está limpio (ignorando version.txt)$(NC)"; \
	fi

.PHONY: check-main-branch
check-main-branch: ## Verifica que estés en la rama main
	@echo "$(GREEN)🔍 Verificando rama actual...$(NC)"
	@CURRENT_BRANCH=$$(git branch --show-current); \
	if [ "$$CURRENT_BRANCH" != "main" ]; then \
		echo "$(RED)❌ Error: No estás en la rama main (actual: $$CURRENT_BRANCH)$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✅ Estás en la rama main$(NC)"

.PHONY: pre-release-checks
pre-release-checks: check-main-branch check-git-clean ## Ejecuta todas las verificaciones antes del release
	@echo "$(GREEN)✅ Todas las verificaciones pasaron$(NC)"

.PHONY: tag
tag: pre-release-checks ## Crea solo el tag (sin push)
	@echo "$(GREEN)🏷️  Creando tag $(VERSION)...$(NC)"
	@if git tag -l | grep -q "^$(VERSION)$$"; then \
		echo "$(RED)❌ Error: El tag $(VERSION) ya existe$(NC)"; \
		exit 1; \
	fi
	git tag -a $(VERSION) -m "Release $(VERSION)"
	@echo "$(GREEN)✅ Tag $(VERSION) creado$(NC)"

.PHONY: push
push: pre-release-checks ## Hace push del código, crea tag y release completo
	@echo "$(GREEN)🏷️  Creando tag $(VERSION)...$(NC)"
	@if git tag -l | grep -q "^$(VERSION)$$"; then \
		echo "$(RED)❌ Error: El tag $(VERSION) ya existe$(NC)"; \
		exit 1; \
	fi
	git tag -a $(VERSION) -m "Release $(VERSION)"
	@echo "$(GREEN)✅ Tag $(VERSION) creado$(NC)"
	@echo "$(GREEN)🚀 Haciendo push del código...$(NC)"
	git push origin main
	@echo "$(GREEN)🚀 Haciendo push de los tags...$(NC)"
	git push origin --tags
	@echo "$(GREEN)🎉 Release $(VERSION) completado exitosamente!$(NC)"
	@echo "$(BLUE)Para verificar: git ls-remote --tags origin$(NC)"

.PHONY: push-only
push-only: ## Hace solo push del código y tags (sin crear nuevo tag)
	@echo "$(GREEN)🚀 Haciendo push del código...$(NC)"
	git push origin main
	@echo "$(GREEN)🚀 Haciendo push de los tags...$(NC)"
	git push origin --tags
	@echo "$(GREEN)✅ Push completado$(NC)"

.PHONY: release
release: push ## Crea tag y hace push (versión completa) - alias de push

.PHONY: quick-release
quick-release: ## Release rápido con VERSION por defecto
	@echo "$(GREEN)🚀 Iniciando release rápido con versión $(VERSION)...$(NC)"
	$(MAKE) release VERSION=$(VERSION)

# Información del proyecto
.PHONY: info
info: ## Muestra información del proyecto
	@echo "$(BLUE)Información del proyecto:$(NC)"
	@echo "  Nombre: $(PROJECT_NAME)"
	@echo "  Módulo: $(MODULE_NAME)"
	@echo "  Versión Go: $(GO_VERSION)"
	@echo "  Versión por defecto: $(VERSION)"
	@echo "  Rama actual: $$(git branch --show-current)"
	@echo "  Último commit: $$(git log -1 --oneline)"
	@echo "  Tags existentes: $$(git tag -l | tail -5 | tr '\n' ' ')"

# Tareas para ejemplos
.PHONY: build-examples
build-examples: ## Compila todos los ejemplos
	@echo "$(GREEN)🔨 Compilando ejemplos...$(NC)"
	@echo "$(BLUE)  → Compilando command example...$(NC)"
	cd examples/client/command-example && go build -o command-example main.go
	@echo "$(BLUE)  → Compilando function example...$(NC)"
	cd examples/client/function-example && go build -o function-example main.go
	@echo "$(BLUE)  → Compilando sql example...$(NC)"
	cd examples/client/sql-example && go build -o sql-example main.go
	@echo "$(BLUE)  → Compilando server example básico...$(NC)"
	cd examples/server/basic && go build -o server main.go
	@echo "$(BLUE)  → Compilando server example avanzado...$(NC)"
	cd examples/server && go build -o advanced_server main.go
	@echo "$(BLUE)  → Compilando cache server example...$(NC)"
	cd examples/server && go build -o cache-server main.go
	@echo "$(BLUE)  → Compilando validation server example...$(NC)"
	cd examples/server && go build -o validation-server main.go
	@echo "$(BLUE)  → Compilando full-featured server example...$(NC)"
	cd examples/server && go build -o full-featured-server main.go
	@echo "$(GREEN)✅ Ejemplos compilados$(NC)"


.PHONY: run-server-example
run-server-example: ## Ejecuta el ejemplo del servidor básico
	@echo "$(GREEN)🚀 Ejecutando server example básico...$(NC)"
	cd examples/server/basic && go run main.go

.PHONY: run-server-advanced
run-server-advanced: ## Ejecuta el servidor avanzado con configuración personalizable
	@echo "$(GREEN)🚀 Ejecutando server avanzado...$(NC)"
	cd examples/server && go run main.go

.PHONY: run-server-cache
run-server-cache: ## Ejecuta el servidor con configuración avanzada de cache
	@echo "$(GREEN)🚀 Ejecutando servidor con cache avanzado...$(NC)"
	cd examples/server && go run main.go -cache-size=2000 -cache-ttl=10m -workers=30 -rate-limit=100

.PHONY: run-server-validation
run-server-validation: ## Ejecuta el servidor con validación SQL estricta
	@echo "$(GREEN)🚀 Ejecutando servidor con validación SQL...$(NC)"
	cd examples/server && go run main.go -strict-mode=true -max-query-length=5000 -workers=20

.PHONY: run-server-full
run-server-full: ## Ejecuta el servidor con todas las características empresariales
	@echo "$(GREEN)🚀 Ejecutando servidor empresarial completo...$(NC)"
	cd examples/server && go run main.go -cache-enabled=true -validation-enabled=true -workers=25 -monitoring-enabled=true

.PHONY: run-command-example
run-command-example: ## Ejecuta el ejemplo de comando
	@echo "$(GREEN)🚀 Ejecutando command example...$(NC)"
	cd examples/client/command-example && go run main.go

.PHONY: run-function-example
run-function-example: ## Ejecuta el ejemplo de función
	@echo "$(GREEN)🚀 Ejecutando function example...$(NC)"
	cd examples/client/function-example && go run main.go

.PHONY: run-sql-example
run-sql-example: ## Ejecuta el ejemplo de SQL
	@echo "$(GREEN)🚀 Ejecutando sql example...$(NC)"
	cd examples/client/sql-example && go run main.go

.PHONY: run-transaction-example
run-transaction-example: ## Ejecuta el ejemplo de transacciones
	@echo "$(GREEN)🚀 Ejecutando transaction example...$(NC)"
	cd examples/client/transaction-example && go run main.go

.PHONY: run-cache-example
run-cache-example: ## Ejecuta el ejemplo de cache de queries
	@echo "$(GREEN)🚀 Ejecutando cache example...$(NC)"
	cd examples/client/cache-example && go run main.go

.PHONY: run-validation-example
run-validation-example: ## Ejecuta el ejemplo de validación SQL
	@echo "$(GREEN)🚀 Ejecutando validation example...$(NC)"
	cd examples/client/validation-example && go run main.go

.PHONY: run-extended-client-example
run-extended-client-example: ## Ejecuta el ejemplo de cliente extendido
	@echo "$(GREEN)🚀 Ejecutando extended client example...$(NC)"
	cd examples/client/extended-client-example && go run main.go

.PHONY: run-client-examples
run-client-examples: ## Ejecuta todos los ejemplos de cliente
	@echo "$(GREEN)🚀 Ejecutando todos los ejemplos de cliente...$(NC)"
	@echo "$(BLUE)  → Ejecutando extended client example...$(NC)"
	cd examples/client/extended-client-example && go run main.go
	@echo "$(BLUE)  → Ejecutando command example...$(NC)"
	cd examples/client/command-example && go run main.go
	@echo "$(BLUE)  → Ejecutando function example...$(NC)"
	cd examples/client/function-example && go run main.go
	@echo "$(BLUE)  → Ejecutando sql example...$(NC)"
	cd examples/client/sql-example && go run main.go
	@echo "$(BLUE)  → Ejecutando transaction example...$(NC)"
	cd examples/client/transaction-example && go run main.go
	@echo "$(BLUE)  → Ejecutando cache example...$(NC)"
	cd examples/client/cache-example && go run main.go
	@echo "$(BLUE)  → Ejecutando validation example...$(NC)"
	cd examples/client/validation-example && go run main.go

.PHONY: docker-up
docker-up: ## Levanta el entorno Docker para el servidor empresarial completo
	@echo "$(GREEN)🐳 Levantando entorno Docker para full-featured server...$(NC)"
	cd examples/server && docker-compose -f docker-compose-full.yml up -d
	@echo "$(GREEN)✅ Entorno Docker empresarial completo iniciado$(NC)"

.PHONY: docker-down
docker-down: ## Detiene el entorno Docker
	@echo "$(GREEN)🐳 Deteniendo entorno Docker...$(NC)"
	cd examples/server && docker-compose -f docker-compose-full.yml down
	@echo "$(GREEN)✅ Entorno Docker detenido$(NC)"

.PHONY: docker-logs
docker-logs: ## Muestra logs del entorno Docker
	@echo "$(GREEN)📋 Mostrando logs de Docker...$(NC)"
	cd examples/server && docker-compose -f docker-compose-full.yml logs -f

.PHONY: docker-logs-app
docker-logs-app: ## Muestra logs del entorno Docker
	@echo "$(GREEN)📋 Mostrando logs de Docker...$(NC)"
	cd examples/server && docker-compose -f docker-compose-full.yml logs -f app

.PHONY: docker-clean
docker-clean: ## Limpia el entorno Docker
	@echo "$(GREEN)🧹 Limpiando entorno Docker...$(NC)"
	cd examples/server && docker rm -f server-app && docker rmi -f server-app
	@echo "$(GREEN)✅ Entorno Docker limpiado$(NC)"

.PHONY: clean-examples
clean-examples: ## Limpia binarios de ejemplos
	@echo "$(GREEN)🧹 Limpiando ejemplos...$(NC)"
	rm -f examples/client/command-example/command-example
	rm -f examples/client/function-example/function-example
	rm -f examples/client/sql-example/sql-example
	rm -f examples/server/basic/server
	rm -f examples/server/server
	@echo "$(GREEN)✅ Ejemplos limpiados$(NC)"

.PHONY: list-examples
list-examples: ## Lista todos los ejemplos disponibles
	@echo "$(BLUE)📋 Ejemplos disponibles:$(NC)"
	@echo "$(GREEN)Client Examples:$(NC)"
	@echo "  - extended-client-example: Demuestra el cliente extendido con interfaz mejorada"
	@echo "  - command-example: Ejecuta comandos remotos"
	@echo "  - function-example: Ejecuta funciones remotas"
	@echo "  - sql-example: Ejecuta consultas SQL remotas"
	@echo "$(GREEN)Server Examples:$(NC)"
	@echo "  - server-example: Servidor básico que procesa las peticiones"
	@echo "  - server-advanced: Servidor empresarial con características avanzadas"
	@echo "  - cache-server: Servidor optimizado para cache de consultas"
	@echo "  - validation-server: Servidor con validación SQL estricta"
	@echo "  - full-featured-server: Servidor empresarial completo con todas las características"
	@echo ""
	@echo "$(YELLOW)Para ejecutar un ejemplo específico:$(NC)"
	@echo "  make run-extended-client-example"
	@echo "  make run-command-example"
	@echo "  make run-function-example"
	@echo "  make run-sql-example"
	@echo "  make run-transaction-example"
	@echo "  make run-cache-example"
	@echo "  make run-validation-example"
	@echo "  make run-server-example"
	@echo "  make run-server-advanced"
	@echo "  make run-server-cache"
	@echo "  make run-server-validation"
	@echo "  make run-server-full"

.PHONY: demo-command
demo-command: ## Ejecuta una demostración del comando ejemplo
	@echo "$(GREEN)🎬 Demostración del command-example...$(NC)"
	@echo "$(BLUE)  → Ejecutando 'ls -la' remoto...$(NC)"
	cd examples/client/command-example && go run main.go "ls -la"
	@echo "$(BLUE)  → Ejecutando 'ps aux' remoto...$(NC)"
	cd examples/client/command-example && go run main.go "ps aux"

# Target por defecto
.DEFAULT_GOAL := help