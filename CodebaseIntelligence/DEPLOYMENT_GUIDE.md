# Complete Deployment Guide

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Your Local Mac                                               │
│ - Development & Testing                                      │
│ - /Users/ankurshome/Desktop/Hadoop_Parser/                  │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ (1) Transfer via SCP/File Share/Git
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ Client AVD/VDI (Windows Virtual Desktop)                     │
│ - Access to client network & code repositories              │
│ - Can run Windows or have SSH to Azure VM                   │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ (2) Deploy to Azure VM
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ Azure Ubuntu VM (18.04/20.04/22.04)                        │
│ - No GUI (SSH only)                                         │
│ - Python 3.9+                                               │
│ - Mounted file shares with code repos                       │
│                                                             │
│ File Structure:                                             │
│ /home/azureuser/                                           │
│   └── CodebaseIntelligence/    <- Our tool                │
│ /mnt/datashare/                 <- Mounted share           │
│   ├── abinitio/                                            │
│   ├── hadoop/                                              │
│   └── databricks/                                          │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ (3) Copy results back
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ Client VDI/AVD                                              │
│ - Open Excel reports                                        │
│ - Share with stakeholders                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Deployment

### **Phase 1: Local Testing (Your Mac)**

#### 1.1 Test Locally First
```bash
cd /Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence

# Run test
python3 test_system.py

# Verify outputs
ls -lh outputs/test_reports/
```

#### 1.2 Package for Transfer
```bash
cd /Users/ankurshome/Desktop/Hadoop_Parser

# Create archive
tar -czf CodebaseIntelligence.tar.gz CodebaseIntelligence/

# Verify
ls -lh CodebaseIntelligence.tar.gz
```

---

### **Phase 2: Transfer to Client VDI**

#### Option A: Via File Share (Recommended)
```bash
# If client has shared drive accessible from Mac
cp CodebaseIntelligence.tar.gz /Volumes/ClientShare/

# Or use corporate file transfer tool
```

#### Option B: Via SCP/SFTP
```bash
# If you have direct access to VDI
scp CodebaseIntelligence.tar.gz user@client-vdi-ip:/path/to/destination
```

#### Option C: Via Private Git Repository
```bash
cd CodebaseIntelligence
git init
git add .
git commit -m "Initial deployment"

# Push to Azure DevOps or GitHub (private repo)
git remote add origin https://your-repo-url
git push -u origin main

# Then clone on VDI
```

---

### **Phase 3: Setup on Azure Ubuntu VM**

#### 3.1 Connect to Azure VM
```bash
# From client VDI (if Windows)
# Use PuTTY or Windows Terminal

# Or from command line
ssh azureuser@<vm-public-ip-or-dns>

# Or via Azure Bastion (if configured)
```

#### 3.2 Transfer from VDI to VM
```bash
# On VDI: Copy to VM
scp CodebaseIntelligence.tar.gz azureuser@<vm-ip>:/home/azureuser/

# Or if using Azure File Share
# 1. Mount file share on VDI
# 2. Copy archive to file share
# 3. Mount same file share on VM
# 4. Copy from file share to VM
```

#### 3.3 Install on Ubuntu VM
```bash
# SSH into VM
ssh azureuser@<vm-ip>

# Extract
cd /home/azureuser
tar -xzf CodebaseIntelligence.tar.gz
cd CodebaseIntelligence

# Install dependencies
./INSTALL.sh

# Or manual install
pip3 install --user -r requirements.txt
```

#### 3.4 Setup File Shares (Important!)
```bash
# Mount Azure File Share (if using)
sudo mkdir -p /mnt/datashare

# Mount with credentials
sudo mount -t cifs //<storage-account>.file.core.windows.net/<share-name> \
  /mnt/datashare \
  -o username=<storage-account>,password=<access-key>,vers=3.0

# Or add to /etc/fstab for auto-mount
echo "//<storage-account>.file.core.windows.net/<share-name> /mnt/datashare cifs username=<storage-account>,password=<access-key>,vers=3.0 0 0" | sudo tee -a /etc/fstab

# Verify mount
ls /mnt/datashare
```

---

### **Phase 4: Run Analysis on Ubuntu VM**

#### 4.1 First Test Run
```bash
# On Ubuntu VM (via SSH)
cd /home/azureuser/CodebaseIntelligence

# Test with sample data (if you copied sample data)
python3 test_system.py

# Check results
ls -lh outputs/test_reports/
```

#### 4.2 Production Run
```bash
# Run full analysis
python3 run_analysis.py \
  --abinitio-path /mnt/datashare/abinitio \
  --hadoop-path /mnt/datashare/hadoop \
  --mode full \
  --output-dir ./outputs/reports \
  --log-level INFO

# Monitor progress (you'll see output in terminal)
# Takes 5-30 minutes depending on repo size

# Check results
ls -lh outputs/reports/
```

#### 4.3 Using FAWN Output (Hybrid Approach)
```bash
# If you have FAWN Excel files
python3 run_analysis.py \
  --fawn-excel /mnt/datashare/fawn_output/400_commGenIpa.xlsx \
  --hadoop-path /mnt/datashare/hadoop \
  --mode full \
  --output-dir ./outputs/reports
```

---

### **Phase 5: Retrieve Results**

#### Option A: Copy via SCP (From VM to VDI)
```bash
# On VDI Windows: Use WinSCP or command line
scp -r azureuser@<vm-ip>:/home/azureuser/CodebaseIntelligence/outputs/reports ./reports

# On VDI Linux/Mac
scp -r azureuser@<vm-ip>:/home/azureuser/CodebaseIntelligence/outputs/reports ./
```

#### Option B: Azure File Share
```bash
# On VM: Copy to mounted file share
cp -r outputs/reports /mnt/datashare/results/

# On VDI: Access file share and download
# Open File Explorer -> Map Network Drive -> \\<storage>.file.core.windows.net\<share>
```

#### Option C: Azure Blob Storage
```bash
# On VM: Upload to blob
az storage blob upload-batch \
  -d results-container \
  -s outputs/reports \
  --account-name <storage-account>

# On VDI: Download using Azure Storage Explorer or portal
```

---

## Ubuntu VM - Complete Workflow Example

```bash
# ============================================================
# Complete Session Example (Ubuntu VM via SSH)
# ============================================================

# 1. Connect to VM
$ ssh azureuser@finthrive-analysis-vm.eastus.cloudapp.azure.com

# 2. Navigate to tool
azureuser@vm:~$ cd CodebaseIntelligence

# 3. Check mounted file shares
azureuser@vm:~/CodebaseIntelligence$ ls /mnt/datashare
abinitio/  hadoop/  databricks/

# 4. Run analysis (you'll see progress output)
azureuser@vm:~/CodebaseIntelligence$ python3 run_analysis.py \
  --abinitio-path /mnt/datashare/abinitio \
  --hadoop-path /mnt/datashare/hadoop \
  --mode full \
  --output-dir ./outputs/reports

# Output you'll see:
# ================================================================================
# Codebase Intelligence Platform - Starting Analysis
# ================================================================================
#
# ================================================================================
# STEP 1: Parsing Ab Initio Files
# ================================================================================
# INFO: Parsing Ab Initio directory: /mnt/datashare/abinitio
# INFO: Found 12 .mp files
# INFO: Parsing file: 400_commGenIpa.mp
# INFO: Extracted 8 components and 15 flows from 400_commGenIpa.mp
# ...
# ✓ Parsed 12 processes
# ✓ Extracted 156 components
#
# ================================================================================
# STEP 2: Parsing Hadoop Repository
# ================================================================================
# ...
#
# ================================================================================
# STEP 3: Generating Source-to-Target Mappings (STTM)
# ================================================================================
# INFO: Generating STTM for process: 400_commGenIpa
# ✓ Generated STTM for 400_commGenIpa: 42 mappings
# ...
#
# ================================================================================
# ✓ Analysis Complete!
# ✓ Reports saved to: ./outputs/reports
# ================================================================================

# 5. Check generated reports
azureuser@vm:~/CodebaseIntelligence$ ls -lh outputs/reports/
# -rw-r--r-- 1 azureuser azureuser 2.1M Oct 27 14:30 STTM_400_commGenIpa.xlsx
# -rw-r--r-- 1 azureuser azureuser 3.5M Oct 27 14:32 STTM_Cross_System.xlsx
# -rw-r--r-- 1 azureuser azureuser 1.8M Oct 27 14:33 Gap_Analysis_Report.xlsx
# -rw-r--r-- 1 azureuser azureuser 4.2M Oct 27 14:34 Combined_Analysis_Report.xlsx

# 6. Copy to file share (so you can access from VDI)
azureuser@vm:~/CodebaseIntelligence$ cp -r outputs/reports /mnt/datashare/results/

# 7. Exit
azureuser@vm:~/CodebaseIntelligence$ exit
logout
Connection to finthrive-analysis-vm.eastus.cloudapp.azure.com closed.

# ============================================================
# Back on VDI: Open Excel files from file share
# ============================================================
```

---

## Ubuntu VM Setup Script

Create this on the VM for easy setup:

```bash
# Save as setup_vm.sh on the VM
cat > /home/azureuser/setup_vm.sh << 'EOF'
#!/bin/bash

echo "=========================================="
echo "Setting up Codebase Intelligence on Azure VM"
echo "=========================================="

# Update system
echo "[1/6] Updating system..."
sudo apt update -y

# Install Python 3.9+
echo "[2/6] Installing Python..."
sudo apt install python3 python3-pip python3-venv -y

# Install Azure CLI (for blob storage operations)
echo "[3/6] Installing Azure CLI..."
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Install CIFS utils (for file share mounting)
echo "[4/6] Installing CIFS utilities..."
sudo apt install cifs-utils -y

# Create mount point
echo "[5/6] Creating mount points..."
sudo mkdir -p /mnt/datashare

# Install tool dependencies
echo "[6/6] Installing Python dependencies..."
cd /home/azureuser/CodebaseIntelligence
pip3 install --user -r requirements.txt

echo "=========================================="
echo "✓ Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Mount file shares (see DEPLOYMENT_GUIDE.md)"
echo "  2. Run: python3 test_system.py"
echo "  3. Run full analysis with your data"
echo ""
EOF

chmod +x /home/azureuser/setup_vm.sh
```

---

## Common Issues & Solutions

### Issue 1: Permission Denied on File Shares
```bash
# Solution: Check mount permissions
sudo mount -t cifs ... -o username=<user>,password=<pass>,uid=$(id -u),gid=$(id -g)
```

### Issue 2: Python Module Not Found
```bash
# Solution: Use full path or activate venv
python3 -m pip install --user -r requirements.txt
```

### Issue 3: Out of Memory
```bash
# Solution: Process repos one at a time
python3 run_analysis.py --abinitio-path /path1 --mode sttm
python3 run_analysis.py --hadoop-path /path2 --mode sttm
# Then combine for gap analysis
```

### Issue 4: Excel Files Too Large
```bash
# Solution: Generate per-process instead of combined
# The tool already does this - you'll get individual files
```

---

## Security Best Practices

1. **Use SSH Keys** instead of passwords
2. **Restrict VM access** to client network only
3. **Use Azure Key Vault** for credentials
4. **Enable VM disk encryption**
5. **Use Private Endpoints** for file shares
6. **Audit access logs** regularly

---

## Performance Optimization

### For Large Codebases:
```bash
# Process in batches
for dir in /mnt/datashare/hadoop/*/; do
  python3 run_analysis.py --hadoop-path "$dir" --mode parse
done

# Then combine results
python3 combine_results.py --input parsed_results/ --output final_report.xlsx
```

### Use Screen for Long-Running Jobs:
```bash
# Start screen session
screen -S analysis

# Run analysis
python3 run_analysis.py --abinitio-path ... --hadoop-path ... --mode full

# Detach: Ctrl+A, then D
# Reattach later: screen -r analysis
```

---

## Monitoring & Logs

```bash
# View real-time logs
tail -f outputs/logs/app.log

# Check last 100 lines
tail -n 100 outputs/logs/app.log

# Search for errors
grep ERROR outputs/logs/app.log

# Search for specific process
grep "400_commGenIpa" outputs/logs/app.log
```

---

## Backup & Recovery

```bash
# Backup results before new run
cp -r outputs/reports outputs/reports.backup.$(date +%Y%m%d)

# Backup to blob storage
az storage blob upload-batch \
  -d backups \
  -s outputs/reports \
  --pattern "*.xlsx"
```

---

## Summary

✅ **Ubuntu VM with no GUI works perfectly**
✅ **Command line only - generates Excel files**
✅ **Copy Excel files back to Windows VDI to open**
✅ **Uses FAWN + Our Tool = Best of both worlds**
✅ **All processes run in background via SSH**

**You never need to see the output on Ubuntu - just generate and copy files back!**
