# Encryption Guide for archivedir

## Overview

archivedir now supports AES-256-CBC encryption with PBKDF2-SHA256 key derivation for secure backups. Encryption is integrated into the streaming pipeline for minimal memory and disk usage.

## Features

- **AES-256-CBC**: Industry-standard encryption algorithm
- **PBKDF2-SHA256**: Secure key derivation with configurable iterations (default: 100,000)
- **Salt Management**: Random salt generation with metadata storage
- **Streaming Pipeline**: Encrypt data on-the-fly during backup (tar → compress → encrypt)
- **Auto-Detection**: Automatically detects encrypted archives during extraction
- **Metadata Files**: Stores encryption parameters in `.enc` files for easy recovery

## Requirements

- OpenSSL (usually pre-installed on macOS/Linux)
- crypto.py module (included in archivedir)

## Usage

### Creating Encrypted Backups

#### Basic Encryption (Password Prompt)

```bash
python3 archivedir_fast.py backup \
  --source /path/to/data \
  --dest /backup/location \
  --encrypt
```

You'll be prompted to enter and confirm a password.

#### With Password as Argument

```bash
python3 archivedir_fast.py backup \
  --source /path/to/data \
  --dest /backup/location \
  --encrypt \
  --password "your-secure-password"
```

#### With Custom Salt

```bash
python3 archivedir_fast.py backup \
  --source /path/to/data \
  --dest /backup/location \
  --encrypt \
  --password "your-password" \
  --salt "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
```

#### With Custom Iterations

```bash
python3 archivedir_fast.py backup \
  --source /path/to/data \
  --dest /backup/location \
  --encrypt \
  --iterations 200000
```

#### Multi-Part Encrypted Backup

```bash
python3 archivedir_fast.py backup \
  --source /path/to/large-data \
  --dest /backup/location \
  --size 2 \
  --encrypt
```

### Extracting Encrypted Archives

#### Auto-Detection (Metadata from .enc File)

```bash
python3 archivedir_fast.py extract \
  --source /backup/data-20240101_120000.tar.gz.enc \
  --dest /restore/location
```

You'll be prompted for the password. Salt and iterations are loaded from metadata.

#### With Password Argument

```bash
python3 archivedir_fast.py extract \
  --source /backup/data-20240101_120000.tar.gz.enc \
  --dest /restore/location \
  --password "your-password"
```

#### Manual Salt (if metadata missing)

```bash
python3 archivedir_fast.py extract \
  --source /backup/data-20240101_120000.tar.gz.enc \
  --dest /restore/location \
  --password "your-password" \
  --salt "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
```

#### Multi-Part Encrypted Archive

```bash
python3 archivedir_fast.py extract \
  --source "/backup/data-20240101_120000.tar.gz.enc.part_*" \
  --dest /restore/location
```

## Configuration File

You can configure encryption defaults in `config.py`:

```python
# Encryption settings
ENCRYPTION_ENABLED = False  # Enable by default
ENCRYPTION_PASSWORD = None  # Prompt if None
ENCRYPTION_SALT = None      # Generate random if None (32 hex chars)
ENCRYPTION_ITERATIONS = 100000
```

**Priority**: CLI arguments > config.py > interactive prompts > defaults

## Encryption Pipeline

### Backup Pipeline

```
Source Files → tar → pigz/pbzip2 → openssl enc → Split → Output
```

For multi-part backups:

```
Source → tar → compress → encrypt → split (2GB) → part files (.enc)
```

### Extraction Pipeline

```
Encrypted Archive → openssl dec → decompress → tar extract → Files
```

For multi-part extraction:

```
Part files → concat → decrypt → decompress → extract → Files
```

## Metadata Files

When encryption is enabled, a `.enc` metadata file is created:

**Example**: `data-20240101_120000.tar.gz.enc` creates `data-20240101_120000.enc`

**Contents**:

```json
{
  "salt": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "iterations": 100000,
  "algorithm": "AES-256-CBC",
  "kdf": "PBKDF2-SHA256"
}
```

**Important**: Keep this file with your backup! It's needed for decryption if you don't remember the salt.

## Security Best Practices

1. **Strong Passwords**: Use passwords with at least 16 characters, mixed case, numbers, and symbols
2. **Salt Storage**: Keep the `.enc` metadata file secure but accessible
3. **Backup Password**: Store your password securely (password manager recommended)
4. **Iterations**: Higher iterations = slower but more secure (100,000 is a good balance)
5. **Test Decryption**: Always test extracting encrypted backups before relying on them

## Troubleshooting

### "Salt not found in metadata and not provided via --salt"

- The `.enc` metadata file is missing
- Provide the salt manually with `--salt` argument
- If you don't have the salt, the archive cannot be decrypted

### "Decryption failed" or "bad decrypt"

- Wrong password
- Wrong salt
- Wrong iterations count
- Archive corruption

### "crypto.py module not available"

- The `crypto.py` file is missing from the archivedir directory
- Download or recreate the crypto.py module

### OpenSSL not found

```bash
# macOS
brew install openssl

# Linux (Ubuntu/Debian)
sudo apt-get install openssl

# Linux (RHEL/CentOS)
sudo yum install openssl
```

## Performance

Encryption adds minimal overhead:

- CPU: ~5-10% additional (on top of compression)
- Memory: Negligible (streaming pipeline)
- Disk: No temporary files (direct streaming)
- Speed: Typically 100-300 MB/s (depends on CPU)

Multi-part encrypted backups stream directly to output, minimizing disk usage.

## Examples

### Full Encrypted Backup to Google Drive

```bash
python3 archivedir_fast.py backup \
  --source ~/Documents \
  --dest gs://my_folder_id \
  --size 2 \
  --encrypt \
  --compress gz
```

### Encrypted Local Multi-Part Backup

```bash
python3 archivedir_fast.py backup \
  --source /var/data \
  --dest /backup/archive \
  --size 5 \
  --encrypt \
  --password "SecurePass123!" \
  --iterations 200000
```

### Extract Encrypted Archive

```bash
python3 archivedir_fast.py extract \
  --source "/backup/archive-20240101_120000.tar.gz.enc.part_*" \
  --dest /restore/data
```

## Algorithm Details

- **Cipher**: AES-256-CBC (Advanced Encryption Standard, 256-bit key, Cipher Block Chaining mode)
- **KDF**: PBKDF2-SHA256 (Password-Based Key Derivation Function 2 with SHA-256)
- **Salt**: 16 bytes (128 bits) random data
- **Iterations**: 100,000 (configurable)
- **Key Size**: 256 bits
- **Block Size**: 128 bits

## Cloud Streaming Support

Encryption is integrated with cloud streaming:

- **Google Drive**: 2GB encrypted parts uploaded immediately
- **AWS S3**: Streaming encrypted backups (future)
- **OneDrive**: Streaming encrypted backups (future)

Each 2GB part is encrypted before upload, maintaining the streaming pipeline without temporary files.
