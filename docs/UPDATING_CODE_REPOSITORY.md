# Updating the Code Repository

A guide for committing and pushing code changes made in JupyterLab back to the Git repository.

---

## Background: Branch Structure

The repository uses two main branches:

| Branch | Purpose |
|--------|---------|
| `main` | Standard development branch |
| `live` | The branch deployed to the production VM |

When you edit files in JupyterLab on the VM, you're modifying files on the `live` branch. These changes are local only—they are not committed or pushed until you explicitly do so.

---

## Workflow Overview

```
1. Make changes in JupyterLab (on the VM)
   └── Changes are local, uncommitted

2. Log in to the VM via SSH
   └── Use MobaXTerm with PIM credentials

3. Use Git Cola to commit and push
   └── Stage files, write commit message, push to origin

4. Create a Pull Request on GitHub
   └── Merge live → main to sync changes
```

---

## Step 1: Log In to the VM

### Get Your PIM Password

1. Open [CyberArk Password Vault](https://vault.assurant.com/PasswordVault/v10/Accounts)
2. Log in with your Assurant email (SSO should authenticate automatically)
3. Locate your PIM account and copy the password

### Connect via MobaXTerm

1. Download [MobaXTerm](https://mobaxterm.mobatek.net/download.html) if you haven't already
2. Open MobaXTerm
3. Click **Session** in the top-left corner
4. Select **SSH** from the session types
5. In **Remote host**, enter: `atl4lexpd001.cead.prd`
6. Click **OK** to connect
7. Enter your PIM username when prompted (e.g., `xwwzn1pim`)
8. Paste your PIM password (use **right-click** to paste in MobaXTerm)

---

## Step 2: Navigate to the Application Directory

Once connected, navigate to the framework directory:

```bash
cd /appdata/irp-notebook-framework
```

---

## Step 3: Stage, Commit, and Push with Git Cola

Launch the Git Cola graphical interface:

```bash
git cola
```

In Git Cola:

1. **Review changes**: The left panel shows modified files. Click on any file to see its diff
2. **Stage files**: Select the files you want to commit and click **Stage** (or right-click → Stage Selected)
3. **Write commit message**: Enter a descriptive commit message in the text field at the bottom
4. **Commit**: Click the **Commit** button
5. **Push**: Click **Push** (or use the menu: Branch → Push) to send your changes to the remote repository

---

## Step 4: Create a Pull Request on GitHub

After pushing to the `live` branch, create a Pull Request to merge your changes into `main`:

1. Go to the repository on GitHub: https://github.com/AIZ-Testing/irp-notebook-framework
2. You may see a banner prompting you to create a PR for recently pushed branches—click it
3. Otherwise, go to **Pull requests** → **New pull request**
4. Set the branches:
   - **base**: `main`
   - **compare**: `live`
5. Review the changes to ensure they're correct
6. Click **Create pull request**
7. Add a title and description summarizing the changes
8. Click **Create pull request** to submit
9. Request review if required, then merge once approved

---

## Reference

For a video walkthrough of this process, see the recorded session: [Risk Modeler Automation with PremiumIQ Testing Session](https://teams.microsoft.com/l/meetingrecap?driveId=b%21523yWqvEjkGxSLYtlW4x731vZi8DRUdGsYL4suKgWEnOBBr_ag9UTqpoRj42PANl&driveItemId=01WSWNZJ3X4A5U6BFSRNCKQCT56QYVJOCZ&sitePath=https%3A%2F%2Fassurantconnects-my.sharepoint.com%2F%3Av%3A%2Fg%2Fpersonal%2Farianna_lamas_assurant_com%2FIQB34DtPBLKLRKgKffQxVLhZAd4t1XqXDyon2hDXeOfYs_I&fileUrl=https%3A%2F%2Fassurantconnects-my.sharepoint.com%2Fpersonal%2Farianna_lamas_assurant_com%2FDocuments%2FRecordings%2FRisk%2520Modeler%2520Automation%2520with%2520PremiumIQ%2520Testing%2520Session-20260126_131020-Meeting%2520Recording.mp4%3Fweb%3D1&iCalUid=040000008200E00074C5B7101A82E00807EA011A20C3D94EC370DC010000000000000000100000004F6F19E6857E5147ADACB9EA485D674D&masterICalUid=040000008200E00074C5B7101A82E0080000000020C3D94EC370DC010000000000000000100000004F6F19E6857E5147ADACB9EA485D674D&threadId=19%3Ameeting_Y2ViZThkNDMtMDZmYS00Y2Q2LTliNjItNDcyZTU3NjMxNGIw%40thread.v2&organizerId=b7c14562-e12e-449d-8359-e31d0151753b&tenantId=354f10a5-0782-4663-8897-8b60747eb8bc&callId=0e9f0b07-2132-4b35-8d65-e4d3268e62c5&threadType=meeting&meetingType=Recurring&subType=RecapSharingLink_RecapCore)
