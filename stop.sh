#!/bin/bash


echo "============================================"
echo " "
echo "     _                                  _   "
echo "    / \   ___ ___ _   _ _ __ __ _ _ __ | |_ "
echo "   / _ \ / __/ __| | | |  __/ _  |  _ \\| __|"
echo "  / ___ \\\\__ \\__ \\ |_| | | | (_| | | | | |_ "
echo " /_/   \_\___/___/\__,_|_|  \__,_|_| |_|\__|"
echo " "
echo "   IRP Notebook Framework"
echo "   Stopping Services..."
echo "============================================"


# Stop Docker containers
docker-compose down

echo ""
echo "Services stopped successfully!"
echo ""
echo "Note: Data volumes are preserved."
echo "To remove all data, run: docker-compose down -v"
echo ""