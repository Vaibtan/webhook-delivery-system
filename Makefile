# Go Webhook Delivery System — developer Makefile.
#
# Windows note: `make` is not installed by default. Use Git Bash + `choco install make`,
# or run the underlying `go` / `docker compose` commands directly (shown in each recipe).
# The local defaults below point at the docker/docker-compose.yml services.

DATABASE_URL ?= postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable
REDIS_URL    ?= redis://localhost:6379/0
ADMIN_API_KEY ?= dev-admin-key
ALLOW_HTTP_URLS ?= true

COMPOSE := docker compose -f docker/docker-compose.yml
ENV := DATABASE_URL="$(DATABASE_URL)" REDIS_URL="$(REDIS_URL)" ADMIN_API_KEY="$(ADMIN_API_KEY)" ALLOW_HTTP_URLS="$(ALLOW_HTTP_URLS)"

.PHONY: help run build test test-race lint vet fmt tidy migrate-up migrate-down \
        docker-up docker-down docker-logs docker-reset

help:
	@echo "Targets:"
	@echo "  run           - run the server locally (migrates on startup)"
	@echo "  build         - compile the server binary to ./bin/server"
	@echo "  test          - go test ./..."
	@echo "  test-race     - go test -race ./..."
	@echo "  lint          - golangci-lint run (if installed)"
	@echo "  vet           - go vet ./..."
	@echo "  fmt           - gofmt -s -w ."
	@echo "  tidy          - go mod tidy"
	@echo "  migrate-up    - apply all migrations"
	@echo "  migrate-down  - roll back one migration step"
	@echo "  docker-up     - start postgres + redis (docker/docker-compose.yml)"
	@echo "  docker-down   - stop postgres + redis"
	@echo "  docker-logs   - tail infra logs"
	@echo "  docker-reset  - stop and wipe infra volumes"

run:
	$(ENV) go run ./cmd/server

build:
	go build -o bin/server ./cmd/server

test:
	$(ENV) go test ./...

test-race:
	$(ENV) go test -race ./...

lint:
	golangci-lint run ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

migrate-up:
	$(ENV) go run ./cmd/server -migrate=up

migrate-down:
	$(ENV) go run ./cmd/server -migrate=down

docker-up:
	$(COMPOSE) up -d

docker-down:
	$(COMPOSE) down

docker-logs:
	$(COMPOSE) logs -f

docker-reset:
	$(COMPOSE) down -v
