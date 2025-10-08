#!/bin/bash

echo "=========================================="
echo "   IRP Notebook Framework"
echo "   Stopping Services..."
echo "=========================================="

# Stop Docker containers
docker-compose down

echo ""
echo "Services stopped successfully!"
echo ""
echo "Note: Data volumes are preserved."
echo "To remove all data, run: docker-compose down -v"
echo ""