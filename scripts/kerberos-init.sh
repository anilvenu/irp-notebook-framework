#!/bin/bash
# =============================================================================
# Kerberos Initialization Script for IRP Notebook Framework
# =============================================================================
#
# This script initializes Kerberos credentials for Windows Authentication
# to SQL Server. It runs during container startup.
#
# Environment Variables:
#   KERBEROS_ENABLED  - Set to 'true' to enable Kerberos (default: false)
#   KRB5_REALM        - Kerberos realm (e.g., CORP.COMPANY.COM)
#   KRB5_KDC          - Key Distribution Center hostname
#   KRB5_PRINCIPAL    - Service principal name (e.g., svc_jupyter@CORP.COMPANY.COM)
#   KRB5_KEYTAB       - Path to keytab file (default: /etc/krb5/svc_jupyter.keytab)
#   KRB5_PASSWORD     - Password for kinit (NOT recommended for production)
#
# Usage:
#   This script is called automatically during container startup.
#   It can also be run manually: /usr/local/bin/kerberos-init.sh
#
# =============================================================================

set -e

echo "=== Kerberos Initialization ==="
echo "Timestamp: $(date)"

# Check if Kerberos authentication is enabled
if [ "${KERBEROS_ENABLED}" != "true" ]; then
    echo "Kerberos authentication is disabled (KERBEROS_ENABLED != true)"
    echo "Skipping Kerberos initialization."
    exit 0
fi

echo "Kerberos is enabled. Proceeding with initialization..."

# Validate required environment variables
if [ -z "${KRB5_REALM}" ]; then
    echo "WARNING: KRB5_REALM environment variable not set"
fi

if [ -z "${KRB5_KDC}" ]; then
    echo "WARNING: KRB5_KDC environment variable not set"
fi

# Verify krb5.conf exists
if [ ! -f "/etc/krb5.conf" ]; then
    echo "WARNING: /etc/krb5.conf not found"
    echo "Kerberos may not work without proper configuration."
else
    echo "Kerberos configuration found at /etc/krb5.conf"
fi

# Determine authentication method and principal
PRINCIPAL="${KRB5_PRINCIPAL}"
if [ -z "${PRINCIPAL}" ] && [ -n "${KRB5_REALM}" ]; then
    PRINCIPAL="svc_jupyter@${KRB5_REALM}"
fi

if [ -z "${PRINCIPAL}" ]; then
    echo "ERROR: Cannot determine Kerberos principal."
    echo "Set KRB5_PRINCIPAL or KRB5_REALM environment variable."
    exit 1
fi

echo "Using principal: ${PRINCIPAL}"

# Initialize Kerberos credentials
if [ -n "${KRB5_KEYTAB}" ] && [ -f "${KRB5_KEYTAB}" ]; then
    # Method 1: Use keytab file (preferred for service accounts)
    echo "Initializing Kerberos using keytab: ${KRB5_KEYTAB}"

    if kinit -k -t "${KRB5_KEYTAB}" "${PRINCIPAL}"; then
        echo "Kerberos ticket obtained successfully."
    else
        echo "ERROR: kinit failed. Check keytab file and principal."
        exit 1
    fi

elif [ -n "${KRB5_PASSWORD}" ]; then
    # Method 2: Use password (for development/testing only)
    echo "WARNING: Using password authentication. Not recommended for production."

    if echo "${KRB5_PASSWORD}" | kinit "${PRINCIPAL}"; then
        echo "Kerberos ticket obtained successfully."
    else
        echo "ERROR: kinit failed. Check password and principal."
        exit 1
    fi

else
    echo "WARNING: Neither KRB5_KEYTAB nor KRB5_PASSWORD is set."
    echo "Kerberos ticket will not be initialized automatically."
    echo "You can manually run: kinit ${PRINCIPAL}"
    exit 0
fi

# Display ticket information
echo ""
echo "=== Kerberos Ticket Information ==="
klist || echo "Unable to display ticket information"

echo ""
echo "=== Kerberos Initialization Complete ==="
