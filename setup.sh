#!/bin/bash

# Install dependencies
echo "Installing dependencies..."
go mod tidy

# Build the application
echo "Building the application..."
go build -o chat-service .

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

# Check if Docker services are running
echo "Checking Docker services..."
docker-compose ps

echo "Setup complete! The application is now running."
echo "Service endpoints:"
echo "- Chat service 1: http://localhost:8080"
echo "- Chat service 2: http://localhost:8081"
echo "- Load balancer: http://localhost:80"