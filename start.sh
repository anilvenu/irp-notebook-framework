#!/bin/bash

echo "=========================================="
echo "   IRP Notebook Framework"
echo "   Starting Services..."
echo "=========================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo ".env file missing from the root directory."
    exit 1
fi

# Start Docker containers
docker-compose up -d --build

# Wait for services to be ready
echo ""
echo "Waiting for services to start..."
sleep 10

# Check if services are running
if docker ps | grep -q irp-notebook && docker ps | grep -q irp-sqlserver; then
    echo ""
    echo "Services started successfully!"
    echo ""
    echo "=========================================="
    echo "   JupyterLab: http://localhost:8888"
    echo "   Token: irp2024"
    echo "   SQL Server: localhost:1433"
    echo "=========================================="
    echo ""
    echo "To stop services, run: ./stop.sh"
else
    echo "Failed to start services. Check docker-compose logs."
    exit 1
fi