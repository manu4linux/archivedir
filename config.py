# ==========================================
# Archivedir Configuration
# Priority: CLI Args > Config File > Defaults
# ==========================================

# List of folders or files to backup.
# Example: BACKUP_SOURCES = ["/home/user/docs", "/var/www/html"]
BACKUP_SOURCES = None 

# Destination path (Local path or s3://bucket/key)
# Example: DESTINATION = "/mnt/usb_drive/backups"
DESTINATION = None

# Split size in GB. 
# Default is 3.5 if not set here or via CLI.
SPLIT_SIZE_GB = 3.5

# Compression Level (1 = Fast, 9 = Small)
# Default is 1.
COMPRESSION_LEVEL = 6

# List of patterns to exclude
# Example: EXCLUDES = ["*.log", "*.tmp", "__pycache__"]
EXCLUDES = ["*.log", "*.tmp", "__pycache__"]

# ==========================================
# Extraction Configuration
# ==========================================

# Source pattern for extraction (e.g., "/mnt/usb/data.part_*")
EXTRACT_SOURCE = None

# ==========================================
# Encryption Configuration
# ==========================================

# Enable encryption (True/False)
# If True, archives will be encrypted with AES-256-CBC
ENCRYPTION_ENABLED = False

# Encryption password (used for key derivation)
# Leave as None to prompt interactively during backup/extract
# WARNING: Storing passwords in config is insecure - use for testing only
# ENCRYPTION_PASSWORD = None
ENCRYPTION_PASSWORD = "0123456789abcdef0123456789abcdef"

# Salt for key derivation (hex string, 32 characters = 16 bytes)
# If None, a random salt will be generated and saved with metadata
# Example: ENCRYPTION_SALT = "0123456789abcdef0123456789abcdef"
# ENCRYPTION_SALT = None
ENCRYPTION_SALT = "0123456789abcdef0123456789abcdef"

# Number of PBKDF2 iterations for key derivation (higher = more secure but slower)
# Default: 100000 iterations (reasonable balance)
ENCRYPTION_ITERATIONS = 10000

# Destination folder for extraction
EXTRACT_DEST = None