# Windows Authentication (Kerberos) Support for SQL Server

## Overview

Add Windows Authentication (Kerberos) support to the JupyterLab Docker container for SQL Server connections, while maintaining backward compatibility with existing SQL Server native authentication.

## Development Environment Setup

### Hyper-V Lab Architecture (Single VM)

```
┌─────────────────────────────────────────────────────────────┐
│  Windows 11 Pro Host                                        │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Hyper-V Internal Network: IRP-Lab-Network            │ │
│  │  Subnet: 192.168.100.0/24                             │ │
│  │                                                        │ │
│  │  ┌─────────────────────────────────────────┐          │ │
│  │  │ IRP-ADLAB01                             │          │ │
│  │  │ Windows Server 2022 Evaluation          │          │ │
│  │  │ 192.168.100.10                          │          │ │
│  │  │ - AD DS / KDC / DNS                     │          │ │
│  │  │ - SQL Server (on same VM)               │          │ │
│  │  └─────────────────────────────────────────┘          │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Docker Desktop (WSL2)                                │ │
│  │  - JupyterLab container with Kerberos client          │ │
│  │  - /etc/hosts entry for irplab.local → 192.168.100.10 │ │
│  │  - keytab for svc_jupyter@IRPLAB.LOCAL                │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Note on DNS**: For dev, we use /etc/hosts entries. In production, the container will be on a network that resolves AD hostnames natively, or krb5.conf can use IP addresses directly.

### Step 1: Enable Hyper-V and Create Network

```powershell
# Enable Hyper-V (requires restart)
Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All

# Create internal network
New-VMSwitch -Name "IRP-Lab-Network" -SwitchType Internal

# Configure host adapter
$adapter = Get-NetAdapter | Where-Object {$_.Name -like "*IRP-Lab-Network*"}
New-NetIPAddress -InterfaceIndex $adapter.InterfaceIndex -IPAddress 192.168.100.1 -PrefixLength 24

# Enable NAT for internet access
New-NetNat -Name "IRP-Lab-NAT" -InternalIPInterfaceAddressPrefix 192.168.100.0/24
```

### Step 2: Create Lab VM (IRP-ADLAB01)

```powershell
# Create VM (6GB RAM for AD + SQL Server)
New-VM -Name "IRP-ADLAB01" -MemoryStartupBytes 6GB -Generation 2 `
       -NewVHDPath "C:\Hyper-V\IRP-ADLAB01\IRP-ADLAB01.vhdx" -NewVHDSizeBytes 80GB
Set-VMProcessor -VMName "IRP-ADLAB01" -Count 4
Connect-VMNetworkAdapter -VMName "IRP-ADLAB01" -SwitchName "IRP-Lab-Network"

# Mount Windows Server 2022 Evaluation ISO
Set-VMDvdDrive -VMName "IRP-ADLAB01" -Path "C:\ISO\WindowsServer2022.iso"
```

Download Windows Server 2022 Evaluation: https://www.microsoft.com/en-us/evalcenter/evaluate-windows-server-2022

**Inside the VM after Windows Server installation:**

1. Configure static IP: 192.168.100.10, Subnet: 255.255.255.0, Gateway: 192.168.100.1, DNS: 127.0.0.1
2. Rename computer to IRP-ADLAB01

3. Install AD DS:
```powershell
Install-WindowsFeature -Name AD-Domain-Services -IncludeManagementTools

Install-ADDSForest `
    -DomainName "irplab.local" `
    -DomainNetBIOSName "IRPLAB" `
    -InstallDNS:$true `
    -SafeModeAdministratorPassword (ConvertTo-SecureString "P@ssw0rd123!" -AsPlainText -Force) `
    -Force:$true
# VM will restart
```

4. After restart, create service accounts:
```powershell
# JupyterLab service account
New-ADUser -Name "svc_jupyter" -SamAccountName "svc_jupyter" `
    -UserPrincipalName "svc_jupyter@irplab.local" `
    -AccountPassword (ConvertTo-SecureString "JupyterSvc@123!" -AsPlainText -Force) `
    -PasswordNeverExpires $true -Enabled $true
```

5. Install SQL Server (Developer or Express edition - free):
   - Download from https://www.microsoft.com/en-us/sql-server/sql-server-downloads
   - Choose "Windows authentication mode" during setup
   - SQL Server will run as NT Service account (fine for dev)

6. Register SPN for SQL Server:
```powershell
setspn -A MSSQLSvc/irp-adlab01.irplab.local:1433 "NT Service\MSSQLSERVER"
setspn -A MSSQLSvc/irp-adlab01.irplab.local "NT Service\MSSQLSERVER"
```

7. Create SQL login for JupyterLab service account:
```sql
-- Run in SSMS as administrator
CREATE LOGIN [IRPLAB\svc_jupyter] FROM WINDOWS;
USE [master];
CREATE USER [IRPLAB\svc_jupyter] FOR LOGIN [IRPLAB\svc_jupyter];
ALTER ROLE db_datareader ADD MEMBER [IRPLAB\svc_jupyter];
-- Repeat for other databases as needed
```

### Step 3: Generate Keytab

On the VM (IRP-ADLAB01):
```powershell
mkdir C:\keytabs
ktpass -princ svc_jupyter@IRPLAB.LOCAL `
       -mapuser IRPLAB\svc_jupyter `
       -pass "JupyterSvc@123!" `
       -crypto AES256-SHA1 `
       -ptype KRB5_NT_PRINCIPAL `
       -out C:\keytabs\svc_jupyter.keytab
```

Copy keytab to project: `./keytabs/svc_jupyter.keytab`

### Step 4: Configure DNS in WSL2/Docker

Add hosts entry so the container can resolve AD hostnames:

```bash
# From Windows PowerShell (as Admin), find WSL IP:
wsl hostname -I

# Inside WSL2 (or add to Docker container):
echo "192.168.100.10 irp-adlab01.irplab.local irp-adlab01" | sudo tee -a /etc/hosts
```

Alternatively, add to docker-compose.yml under jupyter service:
```yaml
extra_hosts:
  - "irp-adlab01.irplab.local:192.168.100.10"
  - "irp-adlab01:192.168.100.10"
```

---

## Code Changes

### 1. Dockerfile.jupyter - Add Kerberos Packages

**File:** `Dockerfile.jupyter`

Add after existing apt-get packages:
```dockerfile
    # Kerberos packages for Windows Authentication
    krb5-user \
    libkrb5-3 \
    libgssapi-krb5-2 \
```

Add directory and init script:
```dockerfile
RUN mkdir -p /etc/krb5 && chmod 755 /etc/krb5
COPY scripts/kerberos-init.sh /usr/local/bin/kerberos-init.sh
RUN chmod +x /usr/local/bin/kerberos-init.sh
```

### 2. New File: scripts/kerberos-init.sh

```bash
#!/bin/bash
# Initialize Kerberos credentials from keytab

if [ "${KERBEROS_ENABLED}" != "true" ]; then
    echo "Kerberos disabled, skipping initialization"
    exit 0
fi

if [ -n "${KRB5_KEYTAB}" ] && [ -f "${KRB5_KEYTAB}" ]; then
    PRINCIPAL="${KRB5_PRINCIPAL:-svc_jupyter@${KRB5_REALM}}"
    kinit -k -t "${KRB5_KEYTAB}" "${PRINCIPAL}"
    klist
fi
```

### 3. docker-compose.yml - Add Kerberos Config

Add to `jupyter` service environment:
```yaml
      - KERBEROS_ENABLED=${KERBEROS_ENABLED:-false}
      - KRB5_REALM=${KRB5_REALM:-}
      - KRB5_KDC=${KRB5_KDC:-}
      - KRB5_PRINCIPAL=${KRB5_PRINCIPAL:-}
      - KRB5_KEYTAB=${KRB5_KEYTAB:-/etc/krb5/svc_jupyter.keytab}
```

Add to volumes:
```yaml
      - ./config/krb5.conf:/etc/krb5.conf:ro
      - ./keytabs:/etc/krb5:ro
```

Update command to run kerberos-init.sh before start-notebook.sh.

### 4. workspace/helpers/sqlserver.py - Dual Auth Support

**Modify `get_connection_config()`** to:
- Read `MSSQL_{CONN}_AUTH_TYPE` (default: `SQL`)
- For `SQL` auth: require USER/PASSWORD (existing behavior)
- For `WINDOWS` auth: only require SERVER

**Modify `build_connection_string()`** to:
- For `SQL` auth: use `UID=...;PWD=...;`
- For `WINDOWS` auth: use `Trusted_Connection=yes;`

**Add new functions:**
- `check_kerberos_status()` - Check if valid ticket exists
- `init_kerberos(keytab_path, principal)` - Initialize from keytab

### 5. New File: config/krb5.conf.example

```ini
[libdefaults]
    default_realm = IRPLAB.LOCAL
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    IRPLAB.LOCAL = {
        kdc = irp-adlab01.irplab.local
        admin_server = irp-adlab01.irplab.local
    }

[domain_realm]
    .irplab.local = IRPLAB.LOCAL
    irplab.local = IRPLAB.LOCAL
```

### 6. Update .env.example

Add Kerberos configuration section:
```bash
# Kerberos Configuration
KERBEROS_ENABLED=false
KRB5_REALM=IRPLAB.LOCAL
KRB5_KDC=dc01.irplab.local
KRB5_PRINCIPAL=svc_jupyter@IRPLAB.LOCAL
KRB5_KEYTAB=/etc/krb5/svc_jupyter.keytab

# Per-connection auth type (SQL or WINDOWS)
MSSQL_DATABRIDGE_AUTH_TYPE=SQL
```

### 7. workspace/tests/test_sqlserver.py - New Tests

Add tests for:
- `test_get_connection_config_sql_auth()` - SQL auth still works
- `test_get_connection_config_windows_auth()` - Windows auth config
- `test_build_connection_string_windows_auth()` - Trusted_Connection in string
- `test_check_kerberos_status_disabled()` - Status when disabled

---

## Files to Modify

| File | Changes |
|------|---------|
| `Dockerfile.jupyter` | Add krb5-user, libkrb5-3, libgssapi-krb5-2 packages |
| `docker-compose.yml` | Add Kerberos env vars and volume mounts |
| `workspace/helpers/sqlserver.py` | Add AUTH_TYPE handling, Trusted_Connection, Kerberos helpers |
| `.env.example` | Document Kerberos and AUTH_TYPE variables |
| `workspace/tests/test_sqlserver.py` | Add auth type and Kerberos tests |

## New Files to Create

| File | Purpose |
|------|---------|
| `scripts/kerberos-init.sh` | Initialize Kerberos credentials on container start |
| `config/krb5.conf.example` | Template for Kerberos configuration |
| `keytabs/.gitkeep` | Directory for keytab files (gitignored) |

---

## Implementation Order

### What I Will Implement (Code Changes)
1. Modify `workspace/helpers/sqlserver.py` for dual auth support
2. Update `Dockerfile.jupyter` with Kerberos packages
3. Create `scripts/kerberos-init.sh`
4. Update `docker-compose.yml` with Kerberos config and volumes
5. Create `config/krb5.conf.example`
6. Update `.env.example` with Kerberos variables
7. Add unit tests to `workspace/tests/test_sqlserver.py`
8. Create `keytabs/.gitkeep` and update `.gitignore`

### What You Will Do (Manual Setup)
1. Enable Hyper-V on Windows 11 Pro
2. Create the IRP-ADLAB01 VM with Windows Server 2022 Evaluation
3. Configure AD DS, create service accounts
4. Install SQL Server and configure permissions
5. Generate keytab and copy to `./keytabs/`
6. Create `./config/krb5.conf` from the example
7. Test the integration

---

## Production Deployment Requirements

Document these for your client:

1. **Network Access**: Container must reach KDC (port 88) and SQL Server (port 1433)
2. **Service Account**: AD account with SQL Server permissions
3. **Keytab File**: Generated by AD admin, securely mounted
4. **krb5.conf**: Configured for production AD realm
5. **DNS**: Container must resolve AD hostnames (or use IP in krb5.conf)
