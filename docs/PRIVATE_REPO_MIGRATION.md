# Deployment Guide: Private Repository Access

This guide covers how to migrate the codebase to a private GitHub repository and set up credential-free access from the VM.

## Overview

When moving from a public repository to a client-owned private repository, personal credentials are not suitable for long-term VM access (they expire when consultants leave). Instead, we use **SSH Deploy Keys** which are tied to the machine, not individual users.

The deploy key is configured system-wide so that any user who logs into the VM automatically has git access without additional setup.

## Migration Steps (What We Did)

1. **Created the repository** in the Assurant GitHub system
2. **Generated an SSH key** on the VM
3. **Registered the SSH key** as a Deploy Key in the repo (repo-scoped, push/pull privileges, no expiration)
4. **Configured system-wide SSH** so all users can use the key
5. **Renamed the original repo folder** on the VM
6. **Cloned the new empty repo**
7. **Copied all files** (excluding `.git` and `__pycache__`) to the new folder
8. **Added and pushed** to the new repo

## Detailed Setup Instructions

### Step 1: Create the Private Repository

Create a new empty repository in the client's GitHub organization.

### Step 2: Generate SSH Key on the VM

```bash
sudo mkdir -p /etc/ssh/deploy_keys
sudo ssh-keygen -t ed25519 -C "irp-vm-deploy-key" -f /etc/ssh/deploy_keys/irp_deploy_key -N ""
```

- `-t ed25519`: Uses modern, secure key type
- `-C "irp-vm-deploy-key"`: Comment to identify the key
- `-f /etc/ssh/deploy_keys/irp_deploy_key`: System-wide location
- `-N ""`: Empty passphrase (acceptable for deploy keys scoped to one repo)

### Step 3: Get the Public Key

```bash
sudo cat /etc/ssh/deploy_keys/irp_deploy_key.pub
```

Copy the entire output (starts with `ssh-ed25519`). Note: ed25519 keys are short by design (~80-100 characters) but cryptographically strong.

### Step 4: Add Deploy Key to GitHub Repository

1. Navigate to the private repository on GitHub
2. Go to **Settings** → **Deploy keys** → **Add deploy key**
3. Title: `IRP VM - Production` (or descriptive name for the environment)
4. Paste the public key
5. Check **"Allow write access"** if push access is needed
6. Click **Add key**

### Step 5: Configure System-Wide SSH

Add the deploy key configuration to a dedicated config file (recommended approach per `/etc/ssh/ssh_config`):

```bash
sudo sh -c 'echo "Host github.com
    IdentityFile /etc/ssh/deploy_keys/irp_deploy_key" > /etc/ssh/ssh_config.d/github.conf'
```

Set appropriate permissions so all users can access the config and key:

```bash
sudo chmod 644 /etc/ssh/ssh_config.d/github.conf
sudo chmod 755 /etc/ssh/deploy_keys
```

Set permissions on the key files:

```bash
sudo chmod 644 /etc/ssh/deploy_keys/irp_deploy_key
sudo chmod 644 /etc/ssh/deploy_keys/irp_deploy_key.pub
```

**Note:** If SSH rejects the key due to permissions being too open, use the `GIT_SSH_COMMAND` approach instead. Add to `/etc/environment`:

```bash
sudo sh -c 'echo "GIT_SSH_COMMAND=\"ssh -i /etc/ssh/deploy_keys/irp_deploy_key -o StrictHostKeyChecking=accept-new\"" >> /etc/environment'
```

Users will need to log out and back in for this to take effect.

### Step 6: Verify SSH Authentication

```bash
ssh -T git@github.com
```

Expected output (confirms deploy key is working):
```
Hi <org>/<repo-name>! You've successfully authenticated, but GitHub does not provide shell access.
```

Note: It shows the **repository name**, not a username, confirming repo-scoped deploy key authentication.

### Step 7: Rename Original Folder and Clone New Repo

```bash
mv irp-notebook-framework irp-notebook-framework-original
git clone git@github.com:<org>/irp-notebook-framework.git
```

### Step 8: Copy Files to New Repo

```bash
rsync -av --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' --exclude='.pytest_cache' irp-notebook-framework-original/ irp-notebook-framework/
```

Rsync is idempotent - if interrupted, just run it again and it will skip already-transferred files.

Verify file counts match (excluding cache files):

```bash
find irp-notebook-framework-original -type f ! -path '*/__pycache__/*' ! -path '*/.git/*' ! -name '*.pyc' | wc -l
find irp-notebook-framework -type f ! -path '*/__pycache__/*' ! -path '*/.git/*' ! -name '*.pyc' | wc -l
```

### Step 9: Commit and Push

```bash
cd irp-notebook-framework
git add .
git commit -m "Initial commit - migrate from original repository"
git push origin main
```

## Ongoing Usage

Once configured, any user on the VM can use standard git commands without additional setup:

```bash
git pull origin main
git fetch --all
```

## Security Considerations

Deploy keys are the recommended approach for this use case because:

| Property | Benefit |
|----------|---------|
| **Repo-scoped** | Key only grants access to the single repository it's added to |
| **No expiration** | Does not expire when team members leave |
| **Auditable** | Visible in repository settings, can be revoked anytime |
| **Machine-bound** | Private key stays on the VM, not tied to personal accounts |

## Troubleshooting

### Permission denied (publickey)

Verify the SSH config file exists and has the correct content:

```bash
cat /etc/ssh/ssh_config.d/github.conf
```

Test with verbose output:

```bash
ssh -vT git@github.com
```

Look for lines showing which identity files are being tried.

### Key not found

Ensure the key file exists and is readable:

```bash
ls -la /etc/ssh/deploy_keys/irp_deploy_key
```

### SSH rejects key due to permissions

If you see "Permissions are too open" errors, use the `GIT_SSH_COMMAND` approach described in Step 5.

### Repository not found

- Verify the deploy key was added to the correct repository
- Confirm the repository URL is correct (case-sensitive)
- Check that the key hasn't been removed from GitHub

## Revoking Access

To revoke VM access:

1. Go to the repository on GitHub
2. **Settings** → **Deploy keys**
3. Click **Delete** next to the key

The VM will immediately lose access to the repository.

## Git Author Configuration

The deploy key handles authentication. To set a system-wide default commit author identity:

```bash
sudo git config --system user.name "IRP Deployment"
sudo git config --system user.email "irp-deploy@example.com"
```

Individual users can override this with their own `git config --global` settings if desired.
