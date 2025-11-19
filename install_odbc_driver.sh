#!/bin/bash
# Setup Microsoft ODBC Driver 18 for SQL Server
# Supports: Ubuntu/Debian, SUSE/SLES, RHEL/CentOS

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Microsoft ODBC Driver 18 Setup${NC}"
echo -e "${BLUE}========================================${NC}"

# Detect Linux distribution
detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        VER=$VERSION_ID
        echo -e "${GREEN}Detected OS: $PRETTY_NAME${NC}"
    else
        echo -e "${RED}Cannot detect Linux distribution${NC}"
        exit 1
    fi
}

# Install for Ubuntu/Debian
install_ubuntu() {
    echo -e "${YELLOW}Installing ODBC drivers for Ubuntu/Debian...${NC}"

    # Install prerequisites
    sudo apt-get update
    sudo apt-get install -y curl gnupg apt-transport-https

    # Add Microsoft repository
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc

    # Add repository based on Ubuntu version
    curl https://packages.microsoft.com/config/ubuntu/${VER}/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

    # Update and install
    sudo apt-get update
    sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
    sudo apt-get install -y unixodbc-dev

    echo -e "${GREEN}✓ ODBC drivers installed successfully${NC}"
}

# Install for SUSE/SLES
install_suse() {
    echo -e "${YELLOW}Installing ODBC drivers for SUSE/SLES...${NC}"

    # Determine SLES major version
    SLES_VERSION=$(echo $VER | cut -d. -f1)

    # Add Microsoft repository
    sudo zypper addrepo -fc https://packages.microsoft.com/config/sles/${SLES_VERSION}/prod.repo

    # Refresh repositories
    sudo zypper --gpg-auto-import-keys refresh

    # Install drivers
    sudo ACCEPT_EULA=Y zypper install -y msodbcsql18
    sudo zypper install -y unixODBC-devel

    echo -e "${GREEN}✓ ODBC drivers installed successfully${NC}"
}

# Install for openSUSE
install_opensuse() {
    echo -e "${YELLOW}Installing ODBC drivers for openSUSE...${NC}"

    # Determine openSUSE version
    OPENSUSE_VERSION=$(echo $VER | cut -d. -f1)

    # Add Microsoft repository
    sudo zypper addrepo -fc https://packages.microsoft.com/config/opensuse/${OPENSUSE_VERSION}/prod.repo

    # Refresh repositories
    sudo zypper --gpg-auto-import-keys refresh

    # Install drivers
    sudo ACCEPT_EULA=Y zypper install -y msodbcsql18
    sudo zypper install -y unixODBC-devel

    echo -e "${GREEN}✓ ODBC drivers installed successfully${NC}"
}

# Install for RHEL/CentOS
install_rhel() {
    echo -e "${YELLOW}Installing ODBC drivers for RHEL/CentOS...${NC}"

    # Add Microsoft repository
    curl https://packages.microsoft.com/config/rhel/${VER}/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo

    # Install drivers
    sudo ACCEPT_EULA=Y yum install -y msodbcsql18
    sudo yum install -y unixODBC-devel

    echo -e "${GREEN}✓ ODBC drivers installed successfully${NC}"
}

# Validate installation
validate_installation() {
    echo -e "\n${YELLOW}Validating installation...${NC}"

    # Check odbcinst
    if command -v odbcinst &> /dev/null; then
        echo -e "${GREEN}✓ odbcinst found${NC}"
        echo -e "${BLUE}ODBC Configuration:${NC}"
        odbcinst -j
    else
        echo -e "${RED}✗ odbcinst not found${NC}"
        return 1
    fi

    # Check for ODBC Driver 18
    echo -e "\n${BLUE}Installed ODBC Drivers:${NC}"
    if odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
        echo -e "${GREEN}✓ ODBC Driver 18 for SQL Server is installed${NC}"
        odbcinst -q -d
    else
        echo -e "${RED}✗ ODBC Driver 18 for SQL Server not found${NC}"
        echo -e "${YELLOW}Available drivers:${NC}"
        odbcinst -q -d
        return 1
    fi

    # Test pyodbc import (if Python is available)
    if command -v python3 &> /dev/null; then
        echo -e "\n${YELLOW}Testing Python pyodbc import...${NC}"
        if python3 -c "import pyodbc; print('pyodbc version:', pyodbc.version)" 2>/dev/null; then
            echo -e "${GREEN}✓ Python pyodbc working correctly${NC}"
        else
            echo -e "${YELLOW}⚠ pyodbc not installed or not working${NC}"
            echo -e "${YELLOW}Install with: pip install pyodbc${NC}"
        fi
    fi

    return 0
}

# Main installation logic
main() {
    detect_distro

    echo ""
    case "$OS" in
        ubuntu|debian)
            install_ubuntu
            ;;
        sles|suse)
            install_suse
            ;;
        opensuse|opensuse-leap|opensuse-tumbleweed)
            install_opensuse
            ;;
        rhel|centos|fedora)
            install_rhel
            ;;
        *)
            echo -e "${RED}Unsupported distribution: $OS${NC}"
            echo -e "${YELLOW}Supported distributions: Ubuntu, Debian, SLES, SUSE, openSUSE, RHEL, CentOS${NC}"
            exit 1
            ;;
    esac

    echo ""
    if validate_installation; then
        echo -e "\n${GREEN}========================================${NC}"
        echo -e "${GREEN}Installation completed successfully!${NC}"
        echo -e "${GREEN}========================================${NC}"
        echo -e "\n${YELLOW}Next steps:${NC}"
        echo -e "1. Install Python pyodbc: ${BLUE}pip install pyodbc${NC}"
        echo -e "2. Test connection with: ${BLUE}./validate_odbc.sh${NC}"
    else
        echo -e "\n${RED}========================================${NC}"
        echo -e "${RED}Installation completed with warnings${NC}"
        echo -e "${RED}========================================${NC}"
        exit 1
    fi
}

# Run main function
main