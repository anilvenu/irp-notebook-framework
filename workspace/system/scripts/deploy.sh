#!/bin/bash

set -e

echo "============================================"
echo " "
echo "     _                                  _   "
echo "    / \   ___ ___ _   _ _ __ __ _ _ __ | |_ "
echo "   / _ \ / __/ __| | | |  __/ _  |  _ \\| __|"
echo "  / ___ \\\\__ \\__ \\ |_| | | | (_| | | | | |_ "
echo " /_/   \_\___/___/\__,_|_|  \__,_|_| |_|\__|"
echo " "
echo "   IRP Notebook Framework"
echo "   Installing Containers from GitHub..."
echo "============================================"

GIT_REPO="https://oauth2:$GITHUB_TOKEN@github.com/<project>/<repo>.git"
BRANCH="<branch>"
PROJECT_ROOT_PATH="<path>"
CODE_FOLDER="<repo>"
CODE_ROOT="$PROJECT_ROOT_PATH/$CODE_FOLDER"

sudo docker compose -f "$CODE_ROOT/docker-compose.yaml" down | true

if [ -d $CODE_ROOT ]; then
	  echo "Cleanup"
	  sudo rm -rf "$CODE_ROOT"
fi
sudo mkdir $CODE_ROOT

echo "Cloning the repo..."
sudo git clone -b $BRANCH $GIT_REPO $CODE_ROOT

echo "Copy env file..."
sudo cp "$PROJECT_ROOT_PATH/.env" "$CODE_ROOT/.env"

echo "Start containers"
sudo -E docker compose -f "$CODE_ROOT/docker-compose.yaml" up --build -d

# Wait for services to be ready
echo ""
echo "Waiting for services to start..."
sleep 10

# Check if services are running
if docker ps | grep -q irp-notebook && docker ps | grep -q irp-postgres; then
    echo ""
    echo "Services started successfully!"
    echo ""
    echo "=========================================="
    echo "   JupyterLab: http://localhost:8888"
    echo "   "
    echo "   PostgreSQL Server: localhost:5432"
    echo "=========================================="
    echo ""
    echo "To stop services, run: ./stop.sh"
else
    echo "Failed to start services. Check docker-compose logs."
    exit 1
fi

echo "Done"
