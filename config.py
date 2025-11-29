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

# Destination folder for extraction
EXTRACT_DEST = None