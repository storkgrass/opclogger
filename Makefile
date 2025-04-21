# Makefile for opclogger

APP_NAME := opclogger
SRC := main.go
BIN_DIR := bin

# Default: build for local (Windows or whatever your dev environment is)
build:
	go build -o $(BIN_DIR)/$(APP_NAME) $(SRC)

# Build for Raspberry Pi 4 - 64bit (aarch64)
build-pi64:
	GOOS=linux GOARCH=arm64 go build -o $(BIN_DIR)/$(APP_NAME)-arm64 $(SRC)

# Build for Raspberry Pi 4 - 32bit (armv7)
build-pi32:
	GOOS=linux GOARCH=arm GOARM=7 go build -o $(BIN_DIR)/$(APP_NAME)-armv7 $(SRC)

# Clean binaries
clean:
	rm -rf $(BIN_DIR)/*

# Create bin directory if not exists
prepare:
	mkdir -p $(BIN_DIR)

# Full clean build for Pi 4 (64bit)
pi: prepare clean build-pi64
