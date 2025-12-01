# Cloud Storage Setup Guide

This guide explains how to configure archivedir_fast.py to stream backups directly to cloud storage (AWS S3, Google Drive, or OneDrive) without using local disk space.

## Table of Contents

- [AWS S3 Setup](#aws-s3-setup)
- [Google Drive Setup](#google-drive-setup)
- [OneDrive Setup](#onedrive-setup)
- [Usage Examples](#usage-examples)

---

## AWS S3 Setup

### 1. Install AWS CLI and boto3

```bash
pip install boto3 awscli
```

### 2. Create AWS Account and S3 Bucket

1. Sign up at <https://aws.amazon.com>
2. Go to S3 Console: <https://s3.console.aws.amazon.com>
3. Click "Create bucket"
4. Choose a unique bucket name (e.g., `my-backups-2025`)
5. Select your region
6. Keep default settings and create bucket

### 3. Create IAM User with S3 Access

1. Go to IAM Console: <https://console.aws.amazon.com/iam>
2. Click "Users" → "Add users"
3. Username: `backup-user`
4. Select "Access key - Programmatic access"
5. Click "Next: Permissions"
6. Choose "Attach existing policies directly"
7. Search and select **`AmazonS3FullAccess`** (or create custom policy with `s3:PutObject`, `s3:GetObject` permissions)
8. Complete user creation
9. **Save the Access Key ID and Secret Access Key** (shown only once!)

### 4. Configure AWS Credentials

```bash
# Option 1: Using AWS CLI
aws configure
# Enter:
#   AWS Access Key ID: [your-access-key-id]
#   AWS Secret Access Key: [your-secret-access-key]
#   Default region: us-east-1 (or your preferred region)
#   Default output format: json

# Option 2: Manual configuration
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
EOF

cat > ~/.aws/config << EOF
[default]
region = us-east-1
output = json
EOF
```

### 5. Test S3 Access

```bash
aws s3 ls s3://my-backups-2025/
```

### 6. Backup to S3

```bash
# Stream backup directly to S3 (no local disk usage!)
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest s3://my-backups-2025/backups/2025 \
  --size 2

# Or specify cloud explicitly
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest my-backups-2025/backups/2025 \
  --cloud s3
```

---

## Google Drive Setup

### 1. Install Google API Client

```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

### 2. Create Google Cloud Project

1. Go to <https://console.cloud.google.com>
2. Click "Select a project" → "New Project"
3. Project name: `Backup Tool`
4. Click "Create"

### 3. Enable Google Drive API

1. In your project, go to "APIs & Services" → "Library"
2. Search for "Google Drive API"
3. Click on it and click "Enable"

### 4. Create OAuth 2.0 Credentials

1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "OAuth client ID"
3. If prompted, configure OAuth consent screen:
   - User Type: **External**
   - App name: `Backup Tool`
   - User support email: your email
   - Developer contact: your email
   - Add scope: `../auth/drive.file`
   - Add test users: your Gmail address
   - Save and continue
4. Application type: **Desktop app**
5. Name: `Backup Desktop Client`
6. Click "Create"
7. **Download the JSON file** (click download icon)
8. Save it as `gdrive_credentials.json` in the same directory as `archivedir_fast.py`

### 5. First-Time Authentication

```bash
# Run this once to authenticate (opens browser)
python3 -c "
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os

SCOPES = ['https://www.googleapis.com/auth/drive.file']
creds = None

if os.path.exists('gdrive_token.json'):
    creds = Credentials.from_authorized_user_file('gdrive_token.json', SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file('gdrive_credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    
    with open('gdrive_token.json', 'w') as token:
        token.write(creds.to_json())

print('✅ Authentication successful! Token saved to gdrive_token.json')
"
```

### 6. Get Folder ID (Optional)

To backup to a specific folder:

1. Open Google Drive in browser
2. Navigate to your desired folder
3. Look at the URL: `https://drive.google.com/drive/folders/FOLDER_ID_HERE`
4. Copy the `FOLDER_ID_HERE` part

### 7. Backup to Google Drive

```bash
# Backup to root of My Drive
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest gs:// \
  --size 2

# Backup to specific folder
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest gs://1a2B3c4D5e6F7g8H9i0J \
  --size 2
```

---

## OneDrive Setup

### 1. Install Microsoft Graph SDK

```bash
pip install msal requests
```

### 2. Register App in Azure Portal

1. Go to <https://portal.azure.com>
2. Navigate to "Azure Active Directory" → "App registrations"
3. Click "New registration"
4. Name: `Backup Tool`
5. Supported account types: **Accounts in any organizational directory and personal Microsoft accounts**
6. Redirect URI:
   - Platform: **Public client/native (mobile & desktop)**
   - URI: `http://localhost`
7. Click "Register"

### 3. Configure API Permissions

1. In your app, go to "API permissions"
2. Click "Add a permission"
3. Choose "Microsoft Graph"
4. Select "Delegated permissions"
5. Add these permissions:
   - `Files.ReadWrite.All`
   - `offline_access`
6. Click "Add permissions"
7. Click "Grant admin consent" (if you're admin) or ask your admin

### 4. Get Client ID and Tenant ID

1. In your app overview page, copy:
   - **Application (client) ID**: e.g., `12345678-1234-1234-1234-123456789012`
   - **Directory (tenant) ID**: e.g., `87654321-4321-4321-4321-210987654321`

### 5. Create Configuration File

Create `onedrive_config.json` in the same directory as `archivedir_fast.py`:

```json
{
  "client_id": "YOUR_CLIENT_ID_HERE",
  "tenant_id": "YOUR_TENANT_ID_HERE",
  "scopes": ["Files.ReadWrite.All"]
}
```

### 6. Backup to OneDrive

```bash
# First run will open browser for authentication
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest onedrive://backups/2025 \
  --size 2

# Backup to root
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest onedrive:// \
  --size 2
```

---

## Usage Examples

### Stream to S3 (No Local Disk Space Needed)

```bash
# Full backup to S3
python3 archivedir_fast.py backup \
  --source /Users/myuser \
  --dest s3://my-bucket/backups/full-backup \
  --size 2

# With custom exclusions
python3 archivedir_fast.py backup \
  --source /Users/myuser/projects \
  --dest s3://my-bucket/project-backup \
  --exclude "node_modules/*" \
  --exclude "*.tmp" \
  --size 1
```

### Stream to Google Drive

```bash
# Backup to specific folder
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest gs://1a2B3c4D5e6F7g8H9i0J \
  --size 2
```

### Stream to OneDrive

```bash
# Backup to OneDrive folder
python3 archivedir_fast.py backup \
  --source /Users/myuser/Documents \
  --dest onedrive://My Backups/2025 \
  --size 2
```

### Local Backup (Original Behavior)

```bash
# Still works for local filesystems
python3 archivedir_fast.py backup \
  --source /Users/myuser \
  --dest /Volumes/External/backups \
  --size 2
```

---

## Troubleshooting

### AWS S3

**Error: "Unable to locate credentials"**

```bash
# Check credentials file
cat ~/.aws/credentials

# Reconfigure
aws configure
```

**Error: "Access Denied"**

- Verify IAM user has S3 permissions
- Check bucket policy allows your user
- Verify region matches

### Google Drive

**Error: "invalid_grant"**

```bash
# Delete token and re-authenticate
rm gdrive_token.json
# Run backup again (will trigger browser auth)
```

**Error: "insufficient permissions"**

- Check OAuth consent screen has correct scopes
- Verify app is not in "testing" mode with wrong test users

### OneDrive

**Error: "invalid_client"**

- Verify client_id and tenant_id in config
- Check redirect URI is exactly `http://localhost`

**Error: "insufficient privileges"**

- Admin consent may be required
- Check API permissions are granted

---

## Security Best Practices

1. **Never commit credentials to git**

   ```bash
   # Add to .gitignore
   echo "*.json" >> .gitignore
   echo ".aws/" >> .gitignore
   echo "*_token.json" >> .gitignore
   echo "*_credentials.json" >> .gitignore
   echo "*_config.json" >> .gitignore
   ```

2. **Use environment variables for CI/CD**

   ```bash
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

3. **Rotate credentials regularly**

4. **Use least-privilege permissions**
   - S3: Only `PutObject` for backup bucket
   - GDrive: Only `drive.file` scope (not `drive` full access)
   - OneDrive: Only `Files.ReadWrite.All` (not `Files.ReadWrite`)

5. **Enable MFA on cloud accounts**

---

## Cost Considerations

### AWS S3

- Storage: ~$0.023 per GB/month (Standard)
- Upload: Free
- Download: $0.09 per GB (first 10TB/month)

### Google Drive

- Free: 15 GB
- Google One: $1.99/month for 100GB, $2.99/month for 200GB
- Unlimited API usage (within quota limits)

### OneDrive

- Free: 5 GB
- Microsoft 365: $6.99/month (1TB included)
- OneDrive Standalone: $1.99/month for 100GB

---

## FAQ

**Q: Can I backup to multiple cloud providers simultaneously?**
A: Not currently. Run separate backup commands for each destination.

**Q: What happens if upload fails midway?**
A: S3 multipart uploads can be resumed. GDrive/OneDrive may require restart.

**Q: Can I extract from cloud backups?**
A: Yes! Use the same URL format for extraction:

```bash
python3 archivedir_fast.py extract \
  --source s3://my-bucket/backup.tar.gz.part_* \
  --dest /restore/location
```

**Q: How do I list my S3 backups?**

```bash
aws s3 ls s3://my-bucket/backups/ --recursive
```

**Q: How do I delete old backups?**

```bash
# S3
aws s3 rm s3://my-bucket/backups/old_backup.tar.gz

# GDrive/OneDrive - use web interface
```

---

## Support

For issues or questions:

1. Check this guide first
2. Verify credentials are correct
3. Test cloud access with CLI tools (aws s3 ls, etc.)
4. Check cloud provider's status page for outages
5. Open an issue on GitHub

Happy backing up! ☁️🚀
