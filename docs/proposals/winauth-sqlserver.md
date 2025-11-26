# Windows Authentication (Kerberos) Support for SQL Server

## Overview

Add Windows Authentication (Kerberos) support to the JupyterLab Docker container for SQL Server connections, while maintaining backward compatibility with existing SQL Server native authentication.

---
## Development Environment Setup

### Hyper-V Lab Architecture (Single VM)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Windows 11 Pro Host                                                    │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Hyper-V Internal Network: IRP-Lab-Network (192.168.100.0/24)   │    │
│  │                                                                 │    │
│  │  ┌─────────────────────────────────────────┐                    │    │
│  │  │ IRP-ADLAB01                             │                    │    │
│  │  │ Windows Server 2022 Evaluation          │                    │    │
│  │  │ 192.168.100.10                          │                    │    │
│  │  │ - AD DS / KDC / DNS                     │                    │    │
│  │  │ - SQL Server (on same VM)               │                    │    │
│  │  └─────────────────────────────────────────┘                    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                           ↑                                             │
│                    Port Forwarding                                      │
│                    (netsh portproxy)                                    │
│                    Port 88 (Kerberos)                                   │
│                    Port 1433 (SQL Server)                               │
│                           ↑                                             │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Docker Desktop (WSL2) - Network: 172.18.0.x                    │    │
│  │                                                                 │    │
│  │  ┌─────────────────────────────────────────────────────┐        │    │
│  │  │ irp-notebook container                               │       │    │
│  │  │ - krb5.conf uses host.docker.internal as KDC         │       │    │
│  │  │ - extra_hosts maps irp-adlab01 → host-gateway        │       │    │
│  │  │ - keytab for svc_jupyter@IRPLAB.LOCAL                │       │    │
│  │  └─────────────────────────────────────────────────────┘        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Networking Points**:
- Docker containers cannot directly reach the Hyper-V internal network (192.168.100.x)
- Traffic flows: Container → `host.docker.internal` → Windows Host → Port Forwarding → VM
- `extra_hosts` in docker-compose.yml maps the AD hostname to the Docker host
- In production, DNS resolves AD hostnames natively, so the `extra_hosts` entry is simply ignored

---

## Step 1: Enable Hyper-V (Host Machine)

### Via PowerShell (Admin)
```powershell
Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All
# Restart when prompted
```

### Via GUI
1. Press `Win + R`, type `optionalfeatures`, press Enter
2. Check "Hyper-V" (expand and check all sub-items)
3. Click OK and restart

---

## Step 2: Create Internal Network

### Via PowerShell (Admin)
```powershell
# Check if switch already exists
Get-VMSwitch -Name "IRP-Lab-Network" -ErrorAction SilentlyContinue

# Create only if it doesn't exist
New-VMSwitch -Name "IRP-Lab-Network" -SwitchType Internal

# Configure IP on host adapter (use InterfaceAlias, not InterfaceIndex)
New-NetIPAddress -InterfaceAlias "vEthernet (IRP-Lab-Network)" -IPAddress 192.168.100.1 -PrefixLength 24

# Enable NAT for internet access
New-NetNat -Name "IRP-Lab-NAT" -InternalIPInterfaceAddressPrefix 192.168.100.0/24
```

### Verify Network
```powershell
Get-VMSwitch | Format-Table Name, SwitchType
Get-NetIPAddress -InterfaceAlias "vEthernet (IRP-Lab-Network)"
Get-NetNat
```

---

## Step 3: Download Windows Server 2022 ISO

1. Go to: https://www.microsoft.com/en-us/evalcenter/evaluate-windows-server-2022
2. Fill out the registration form
3. Select **ISO** download, **64-bit edition**, **English**
4. Save to `C:\ISO\WindowsServer2022.iso` (note the real download location)

---

## Step 4: Create Lab VM

### Via PowerShell (Admin)
```powershell
# Create directory for VM files
New-Item -ItemType Directory -Path "C:\Hyper-V\IRP-ADLAB01" -Force

# Create VM (Generation 2 for UEFI/SecureBoot)
New-VM -Name "IRP-ADLAB01" -MemoryStartupBytes 6GB -Generation 2 `
       -NewVHDPath "C:\Hyper-V\IRP-ADLAB01\IRP-ADLAB01.vhdx" -NewVHDSizeBytes 80GB

Set-VMProcessor -VMName "IRP-ADLAB01" -Count 4
Connect-VMNetworkAdapter -VMName "IRP-ADLAB01" -SwitchName "IRP-Lab-Network"
```

### Mount ISO and Start VM (Via Hyper-V Manager GUI)

**Important**: Generation 2 VMs require mounting ISO through Hyper-V Manager for reliable boot.

1. Open Hyper-V Manager:
   - Press `Win + R`, type `virtmgmt.msc`, press Enter

2. Connect to VM:
   - In the left panel, click your computer name
   - In the center panel, right-click **IRP-ADLAB01** → **Connect**

3. Insert Windows Server ISO:
   - In the VM connection window, click **Media** menu → **DVD Drive** → **Insert Disk...**
   - Browse to your Windows Server 2022 ISO file
   - Click **Open**

4. Start the VM:
   - Click the green **Start** button (▶) in the toolbar
   - Click inside the VM window and press any key when "Press any key to boot from CD or DVD" appears

5. Install Windows Server:
   - Select language, click Next
   - Click "Install now"
   - Select **Windows Server 2022 Standard Evaluation (Desktop Experience)**
   - Accept license terms
   - Choose **Custom: Install Windows only**
   - Select the unallocated drive, click Next
   - Wait for installation to complete

6. Set Administrator password when prompted (e.g., `P@ssw0rd123!`)

---

## Step 5: Configure Windows Server (Inside VM)

### 5.1 Configure Static IP

1. Open **Server Manager** (starts automatically)
2. Click **Local Server** in left panel
3. Click the link next to **Ethernet** (shows "IPv4 address assigned by DHCP...")
4. In Network Connections, right-click **Ethernet** → **Properties**
5. Select **Internet Protocol Version 4 (TCP/IPv4)** → **Properties**
6. Select "Use the following IP address":
   - IP address: `192.168.100.10`
   - Subnet mask: `255.255.255.0`
   - Default gateway: `192.168.100.1`
   - Preferred DNS: `127.0.0.1`
7. Click OK, Close

### 5.2 Rename Computer

In Server Manager → Local Server:
1. Click the computer name link (shows random name like "WIN-XXXXX")
2. Click **Change...**
3. Computer name: `IRP-ADLAB01`
4. Click OK, restart when prompted

---

## Step 6: Install Active Directory Domain Services

After restart, open PowerShell as Administrator:

```powershell
# Install AD DS role
Install-WindowsFeature -Name AD-Domain-Services -IncludeManagementTools

# Promote to Domain Controller and create forest
Install-ADDSForest -DomainName "irplab.local" -DomainNetBIOSName "IRPLAB" -InstallDNS:$true -SafeModeAdministratorPassword (ConvertTo-SecureString "P@ssw0rd123!" -AsPlainText -Force) -Force:$true

# VM will restart automatically
```

**Note**: After restart, login as `IRPLAB\Administrator` with your password.

---

## Step 7: Create Service Account

After domain promotion and restart, open PowerShell as Administrator:

```powershell
# Create service account for JupyterLab container
New-ADUser -Name "svc_jupyter" -SamAccountName "svc_jupyter" `
    -UserPrincipalName "svc_jupyter@irplab.local" `
    -AccountPassword (ConvertTo-SecureString "JupyterSvc@123!" -AsPlainText -Force) `
    -PasswordNeverExpires $true -Enabled $true

# Verify
Get-ADUser svc_jupyter
```

---

## Step 8: Install SQL Server

1. Download SQL Server Developer Edition (free):
   https://www.microsoft.com/en-us/sql-server/sql-server-downloads

2. Run installer, choose **Basic** installation

3. After installation, download and install **SQL Server Management Studio (SSMS)**:
   https://docs.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms

### Register SPN for SQL Server

```powershell
# Register SPN on the computer account (note the $ at the end)
setspn -A MSSQLSvc/irp-adlab01.irplab.local:1433 IRP-ADLAB01$
setspn -A MSSQLSvc/irp-adlab01.irplab.local IRP-ADLAB01$

# Verify
setspn -L "NT Service\MSSQLSERVER"
```

### Create SQL Login for Service Account

1. Open **SQL Server Management Studio (SSMS)**
2. Connect with:
   - **Server name**: `localhost`
   - **Authentication**: Windows Authentication
   - *(Uses your current login: IRPLAB\Administrator)*
3. Click **Connect**
4. Click **New Query** (Ctrl+N)
5. Paste and execute (F5):

```sql
-- Create Windows login for the service account
CREATE LOGIN [IRPLAB\svc_jupyter] FROM WINDOWS;

-- Grant access to master (or your target database)
USE [master];
CREATE USER [IRPLAB\svc_jupyter] FOR LOGIN [IRPLAB\svc_jupyter];
ALTER ROLE db_datareader ADD MEMBER [IRPLAB\svc_jupyter];

-- For other databases, repeat:
-- USE [YourDatabase];
-- CREATE USER [IRPLAB\svc_jupyter] FOR LOGIN [IRPLAB\svc_jupyter];
-- ALTER ROLE db_datareader ADD MEMBER [IRPLAB\svc_jupyter];
```

---

## Step 9: Generate Keytab File

On the VM (IRP-ADLAB01), open PowerShell as Administrator:

```powershell
# Create directory for keytabs
New-Item -ItemType Directory -Path "C:\keytabs" -Force

# Generate keytab file
ktpass -princ svc_jupyter@IRPLAB.LOCAL `
       -mapuser IRPLAB\svc_jupyter `
       -pass "JupyterSvc@123!" `
       -crypto AES256-SHA1 `
       -ptype KRB5_NT_PRINCIPAL `
       -out C:\keytabs\svc_jupyter.keytab
```

### Copy Keytab to Project

Transfer the keytab file from the VM to your project's `keytabs/` directory.

1. In the VM, open File Explorer and navigate to `C:\keytabs\`
2. Right-click `svc_jupyter.keytab` → Copy
3. In your Windows host, navigate to the project's `keytabs\` folder
4. Paste the file

---

## Step 10: Configure Docker Container

### Create krb5.conf

```bash
cp config/krb5.conf.example config/krb5.conf
```

The default values in the example match the lab setup (IRPLAB.LOCAL realm).

### Update .env

Add to your `.env` file:

```bash
# Enable Kerberos
KERBEROS_ENABLED=true
KRB5_REALM=IRPLAB.LOCAL
KRB5_KDC=irp-adlab01.irplab.local
KRB5_PRINCIPAL=svc_jupyter@IRPLAB.LOCAL
KRB5_KEYTAB=/etc/krb5/svc_jupyter.keytab
```

### Network Bridge: Docker to Hyper-V VM

**Important**: Docker containers (running in WSL2) cannot directly access the Hyper-V internal network (192.168.100.x). The solution uses port forwarding on the Windows host and Docker's `host.docker.internal` hostname.

#### Step A: Set Up Port Forwarding (Windows PowerShell as Admin)

```powershell
# Forward KDC port (88) and SQL Server port (1433) from Windows host to VM
netsh interface portproxy add v4tov4 listenport=88 listenaddress=0.0.0.0 connectport=88 connectaddress=192.168.100.10
netsh interface portproxy add v4tov4 listenport=1433 listenaddress=0.0.0.0 connectport=1433 connectaddress=192.168.100.10

# Verify the port forwarding rules
netsh interface portproxy show all

# Allow through Windows Firewall
netsh advfirewall firewall add rule name="Kerberos KDC for WSL" dir=in action=allow protocol=tcp localport=88
netsh advfirewall firewall add rule name="SQL Server for WSL" dir=in action=allow protocol=tcp localport=1433
```

#### Step B: Configure krb5.conf

The `config/krb5.conf` file uses `host.docker.internal` which Docker automatically resolves to the Windows host:

```ini
[realms]
    IRPLAB.LOCAL = {
        # host.docker.internal resolves to Windows host from inside container
        # Windows host port-forwards to the Hyper-V VM
        kdc = host.docker.internal
        admin_server = host.docker.internal
        default_domain = irplab.local
    }
```

**Note**: This is already configured in the example file. Just copy it:
```bash
cp config/krb5.conf.example config/krb5.conf
```

#### Step C: docker-compose.yml extra_hosts

The `docker-compose.yml` includes `extra_hosts` to map the AD hostname to the Docker host:

```yaml
extra_hosts:
  - "irp-adlab01.irplab.local:host-gateway"
  - "irp-adlab01:host-gateway"
```

This allows the container to reach the SQL Server at `irp-adlab01.irplab.local` via the Windows host's port forwarding.

**Production Note**: These entries are harmless in production where DNS resolves AD hostnames natively. You don't need to remove them when deploying.

#### Step D: Test Network Connectivity

Verify connectivity from inside the container:

```bash
# Start container first
docker-compose up -d jupyter

# Test from inside container
docker exec irp-notebook bash -c 'timeout 2 bash -c "cat < /dev/tcp/host.docker.internal/88" || echo "KDC port reachable"'
docker exec irp-notebook bash -c 'timeout 2 bash -c "cat < /dev/tcp/host.docker.internal/1433" || echo "SQL port reachable"'
```

If tests fail, check:
1. Port forwarding rules: `netsh interface portproxy show all` (in PowerShell)
2. Windows Firewall: Ensure rules are enabled
3. VM is running: Check Hyper-V Manager

---

## Step 11: Test the Integration

### Rebuild and Start Container

```bash
docker-compose build --no-cache jupyter
docker-compose up -d jupyter
```

### Verify Kerberos Ticket

```bash
docker exec -it irp-notebook bash
klist  # Should show valid ticket for svc_jupyter@IRPLAB.LOCAL
```

### Test SQL Server Connection

In a Jupyter notebook:

```python
from helpers.sqlserver import execute_query

# Test Windows Authentication
df = execute_query(
    "SELECT @@VERSION AS version, SUSER_NAME() AS login_user",
    connection='LAB',
    database='master'
)
print(df)
```

---

## Code Changes Summary


| File | Changes |
|------|---------|
| `workspace/helpers/sqlserver.py` | AUTH_TYPE support, Trusted_Connection, Kerberos helpers |
| `Dockerfile.jupyter` | krb5-user, libkrb5-3, libgssapi-krb5-2 packages |
| `scripts/kerberos-init.sh` | Container startup Kerberos initialization |
| `docker-compose.yml` | Kerberos env vars and volume mounts |
| `config/krb5.conf.example` | Kerberos configuration template |
| `.env.example` | Kerberos configuration documentation |
| `workspace/tests/test_sqlserver.py` | Auth type and Kerberos tests |
| `keytabs/.gitkeep` | Keytab directory (gitignored) |
| `.gitignore` | Exclude keytabs and krb5.conf |

---

## Production Deployment

### What Changes Between Dev and Prod

| Item | Dev (WSL2/Hyper-V) | Production (Linux VM) |
|------|-------------------|----------------------|
| `.env` | Lab credentials/realm | Production credentials/realm |
| `config/krb5.conf` | `host.docker.internal` as KDC | Actual KDC hostname or IP |
| `keytabs/` | Lab keytab file | Production keytab file |
| `docker-compose.yml` | No changes needed | No changes needed |
| `extra_hosts` | Required for routing | Harmless (ignored by DNS) |
| Port forwarding | Required | Not needed |

### start.sh / stop.sh Scripts

**No changes required**. These scripts work identically in both environments:

```bash
# start.sh - works the same everywhere
docker compose up -d

# stop.sh - works the same everywhere
docker compose down
```

The only per-environment customization is:
- Different `.env` file (credentials, realm)
- Different `config/krb5.conf` (KDC hostname)
- Different keytab file in `keytabs/`

### Production Requirements

Document these for your infrastructure team:

1. **Network Access**: Container must reach KDC (port 88) and SQL Server (port 1433)
2. **Service Account**: AD account with SQL Server permissions
3. **Keytab File**: Generated by AD admin, securely mounted
4. **krb5.conf**: Configured for production AD realm (use actual KDC hostname)
5. **DNS**: Container should be on network that resolves AD hostnames

### Example Production krb5.conf

```ini
[libdefaults]
    default_realm = CORP.COMPANY.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true

[realms]
    CORP.COMPANY.COM = {
        kdc = dc01.corp.company.com
        admin_server = dc01.corp.company.com
        default_domain = corp.company.com
    }

[domain_realm]
    .corp.company.com = CORP.COMPANY.COM
    corp.company.com = CORP.COMPANY.COM
```

### Example Production .env

```bash
KERBEROS_ENABLED=true
KRB5_REALM=CORP.COMPANY.COM
KRB5_PRINCIPAL=svc_jupyter@CORP.COMPANY.COM
KRB5_KEYTAB=/etc/krb5/svc_jupyter.keytab

MSSQL_PROD_AUTH_TYPE=WINDOWS
MSSQL_PROD_SERVER=sqlserver.corp.company.com
```

---

## Troubleshooting

### VM Won't Boot from ISO

**Symptom**: "No operating system was loaded" or PXE boot errors

**Solution**: Use Hyper-V Manager GUI to insert ISO:
1. Open Hyper-V Manager (`virtmgmt.msc`)
2. Connect to VM
3. Media → DVD Drive → Insert Disk...
4. Start VM and press key quickly when prompted

### Network Configuration Errors

**"Invalid parameter InterfaceIndex"**: Use `-InterfaceAlias` instead:
```powershell
New-NetIPAddress -InterfaceAlias "vEthernet (IRP-Lab-Network)" -IPAddress 192.168.100.1 -PrefixLength 24
```

**"File already exists"**: Check if network already exists:
```powershell
Get-VMSwitch
Get-NetNat
```

### WSL2 Cannot Reach Hyper-V VM

**Symptom**: `ping 192.168.100.10` from WSL shows 100% packet loss

**Cause**: WSL2 runs in a separate virtual network (172.17.x.x) that cannot directly access the Hyper-V internal network (192.168.100.x).

**Solution**: Set up port forwarding on Windows host. See "WSL2 to Hyper-V Network Bridge" section above.

### kerberos-init.sh Permission Denied

**Symptom**: Container logs show `/bin/bash: /usr/local/bin/kerberos-init.sh: Permission denied`

**Cause**: Docker image was built before the Dockerfile added `chmod +x` for the script.

**Solution**: Rebuild the Docker image:
```bash
docker-compose build --no-cache jupyter
docker-compose up -d jupyter
```

### Kerberos Ticket Errors

**klist shows "No credentials cache found"**:
1. Check container logs: `docker logs irp-notebook 2>&1 | grep -i kerberos`
2. Verify keytab is mounted: `docker exec irp-notebook ls -la /etc/krb5/`
3. Verify krb5.conf is mounted: `docker exec irp-notebook cat /etc/krb5.conf`
4. Verify environment variables: `docker exec irp-notebook printenv | grep KRB`
5. Test network to KDC: `docker exec irp-notebook bash -c 'timeout 2 bash -c "cat < /dev/tcp/host.docker.internal/88" || echo "Cannot reach KDC"'`

**kinit fails with "Cannot contact any KDC"**:
- Verify krb5.conf uses `host.docker.internal` (not an IP that container can't reach)
- Verify port forwarding is set up (see Network Bridge section)
- Verify Windows Firewall allows port 88
- Verify VM is running and AD DS service is started

**kinit fails with "Preauthentication failed"**:
- Keytab password may not match AD account password
- Regenerate keytab on the domain controller using `ktpass`

**kerberos-init.sh shows "bad interpreter" or CRLF errors**:
- Windows line endings in the script file
- Fix with: `sed -i 's/\r$//' scripts/kerberos-init.sh`
- Then rebuild: `docker-compose build --no-cache jupyter`

### SQL Server Connection Fails

**"Login timeout expired" from Docker container**:

This usually means the container can't reach SQL Server on port 1433. Debug step-by-step:

1. **Check if SQL Server TCP/IP is enabled** (on VM):
   - Open SQL Server Configuration Manager
   - Go to "SQL Server Network Configuration" → "Protocols for MSSQLSERVER"
   - TCP/IP must be **Enabled**
   - Right-click TCP/IP → Properties → IP Addresses → scroll to **IPAll**
   - Set TCP Port to `1433`
   - **Restart SQL Server service** after changes:
     ```powershell
     Restart-Service -Name MSSQLSERVER
     ```

2. **Test SQL Server locally on VM**:
   ```powershell
   Test-NetConnection -ComputerName localhost -Port 1433
   ```
   If `TcpTestSucceeded: False`, SQL Server isn't listening on TCP.

3. **Open VM firewall for SQL Server** (on VM):
   ```powershell
   netsh advfirewall firewall add rule name="SQL Server" dir=in action=allow protocol=tcp localport=1433
   ```

4. **Test Windows host → VM connection** (on Windows host, not VM):
   ```powershell
   Test-NetConnection -ComputerName 192.168.100.10 -Port 1433
   ```
   If this fails, the VM firewall is blocking or SQL Server isn't listening.

5. **Test Docker → Windows host** (check what IPs are used):
   ```bash
   # Check what host-gateway resolves to (should be IPv4)
   docker exec irp-notebook getent hosts irp-adlab01.irplab.local

   # Test connection using that hostname
   docker exec irp-notebook bash -c 'timeout 2 bash -c "cat < /dev/tcp/irp-adlab01.irplab.local/1433" && echo "Connected" || echo "Failed"'
   ```

**IPv6 vs IPv4 port forwarding issue**:

`netsh portproxy` only works with IPv4. If `host.docker.internal` resolves to IPv6 (e.g., `fdc4:f303:9324::254`), port forwarding won't work.

- **Solution**: Use the AD hostname (`irp-adlab01.irplab.local`) in your connection string instead of `host.docker.internal`. The `extra_hosts` in docker-compose.yml maps it to `host-gateway`, which uses IPv4.
- **In notebook**:
  ```python
  os.environ['MSSQL_LAB_SERVER'] = 'irp-adlab01.irplab.local'  # Uses host-gateway (IPv4)
  # NOT: os.environ['MSSQL_LAB_SERVER'] = 'host.docker.internal'  # May use IPv6
  ```

**"Login failed"**: Verify:
- SPN is registered: `setspn -L IRP-ADLAB01$` (note the $ for computer account)
- SQL login exists: Check SSMS → Security → Logins
- Kerberos ticket is valid: `klist` in container

**"Cannot generate SSPI context"**:
- Hostname in connection string must match SPN
- Verify hosts file resolves hostname correctly: `getent hosts irp-adlab01.irplab.local`

### Port Forwarding Verification

**Check port forwarding is configured** (Windows host PowerShell):
```powershell
netsh interface portproxy show all
```

Should show:
```
Listen on ipv4:             Connect to ipv4:
Address         Port        Address         Port
--------------- ----------  --------------- ----------
0.0.0.0         88          192.168.100.10  88
0.0.0.0         1433        192.168.100.10  1433
```

**Check what's listening on port 1433** (Windows host):
```powershell
netstat -ano | findstr :1433
```

If you see `SYN_SENT` state to `192.168.100.10:1433`, the VM isn't responding (SQL Server not listening or VM firewall blocking).

---

## Verified Working Setup

When everything is configured correctly, you should see:

**1. Kerberos ticket in container:**
```bash
$ docker exec -it irp-notebook bash -c "klist"
Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: svc_jupyter@IRPLAB.LOCAL

Valid starting       Expires              Service principal
11/26/2025 22:01:31  11/27/2025 08:01:31  krbtgt/IRPLAB.LOCAL@IRPLAB.LOCAL
        renew until 12/03/2025 22:01:30
```

**2. Container startup logs showing successful initialization:**
```
=== Kerberos Initialization ===
Kerberos is enabled. Proceeding with initialization...
Using principal: svc_jupyter@IRPLAB.LOCAL
Initializing Kerberos using keytab: /etc/krb5/svc_jupyter.keytab
Kerberos ticket obtained successfully.
=== Kerberos Initialization Complete ===
```

**3. SQL Server connection test (in Jupyter notebook):**
```python
from helpers.sqlserver import execute_query

df = execute_query(
    "SELECT @@VERSION AS version, SUSER_NAME() AS login_user",
    connection='LAB',
    database='master'
)
print(df)
# Should show SQL Server version and 'IRPLAB\svc_jupyter' as login_user
```
