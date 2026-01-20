.PHONY: build install clean test lint run

# Binary name
BINARY_NAME=providerd

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
all: build

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/providerd

# Install the binary to GOPATH/bin
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOCMD) install $(LDFLAGS) ./cmd/providerd

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
	$(BUILD_DIR)/$(BINARY_NAME) --config config.example.yaml

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
	@echo "  build          - Build the binary"
	@echo "  install        - Install the binary to GOPATH/bin"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download and tidy dependencies"
	@echo "  test           - Run tests"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  lint           - Run linter"
	@echo "  run            - Build and run with example config"
	@echo "  fmt            - Format code"
	@echo "  generate       - Generate mocks"
	@echo "  verify         - Verify dependencies"
