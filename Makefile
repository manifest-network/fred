.PHONY: all build build-mock build-docker install clean deps test test-integration test-coverage lint run run-mock run-mock-delay run-docker fmt generate verify help

# Binary names
BINARY_NAME=providerd
MOCK_BINARY_NAME=mock-backend
DOCKER_BINARY_NAME=docker-backend

# Build directory
BUILD_DIR=./build

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOVET=$(GOCMD) vet

# Build flags
LDFLAGS=-ldflags "-s -w"

# Default target
all: build build-mock build-docker

# Build providerd
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/providerd

# Build mock-backend
build-mock:
	@echo "Building $(MOCK_BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(MOCK_BINARY_NAME) ./cmd/mock-backend

# Build docker-backend
build-docker:
	@echo "Building $(DOCKER_BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(DOCKER_BINARY_NAME) ./cmd/docker-backend

# Install the binaries to GOPATH/bin
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOCMD) install $(LDFLAGS) ./cmd/providerd
	@echo "Installing $(MOCK_BINARY_NAME)..."
	$(GOCMD) install $(LDFLAGS) ./cmd/mock-backend
	@echo "Installing $(DOCKER_BINARY_NAME)..."
	$(GOCMD) install $(LDFLAGS) ./cmd/docker-backend

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@$(GOCMD) clean

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run Docker integration tests (requires Docker daemon)
test-integration:
	@echo "Running Docker integration tests..."
	$(GOTEST) -tags integration -v ./internal/backend/docker/ -run Integration

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	@echo "Running linter..."
	$(GOVET) ./...
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping"; \
	fi

# Run the daemon with example config
run: build
	@echo "Running $(BINARY_NAME)..."
	@exec $(BUILD_DIR)/$(BINARY_NAME) --config config.example.yaml

# Run the mock backend
run-mock: build-mock
	@echo "Running $(MOCK_BINARY_NAME)..."
	@exec $(BUILD_DIR)/$(MOCK_BINARY_NAME)

# Run mock backend with delay (for testing async provisioning)
run-mock-delay: build-mock
	@echo "Running $(MOCK_BINARY_NAME) with 2s delay..."
	@MOCK_BACKEND_DELAY=2s exec $(BUILD_DIR)/$(MOCK_BINARY_NAME)

# Run the docker backend
run-docker: build-docker
	@echo "Running $(DOCKER_BINARY_NAME)..."
	@exec $(BUILD_DIR)/$(DOCKER_BINARY_NAME) --config docker-backend.example.yaml

# Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Generate mocks (if using mockgen)
generate:
	@echo "Generating mocks..."
	$(GOCMD) generate ./...

# Verify dependencies
verify:
	@echo "Verifying dependencies..."
	$(GOMOD) verify

# Help
help:
	@echo "Available targets:"
	@echo "  build            - Build providerd"
	@echo "  build-mock       - Build mock-backend for testing"
	@echo "  build-docker     - Build docker-backend"
	@echo "  install          - Install binaries to GOPATH/bin"
	@echo "  clean            - Clean build artifacts"
	@echo "  deps             - Download and tidy dependencies"
	@echo "  test             - Run tests"
	@echo "  test-integration - Run Docker integration tests (requires Docker)"
	@echo "  test-coverage    - Run tests with coverage report"
	@echo "  lint             - Run linter"
	@echo "  run              - Build and run providerd with example config"
	@echo "  run-mock         - Build and run mock-backend"
	@echo "  run-mock-delay   - Run mock-backend with 2s provisioning delay"
	@echo "  run-docker       - Build and run docker-backend with example config"
	@echo "  fmt              - Format code"
	@echo "  generate         - Generate mocks"
	@echo "  verify           - Verify dependencies"
