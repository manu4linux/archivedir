#!/usr/bin/env python3
"""
Fast Archive Directory Tool - Using native bash commands for better performance
Leverages system tar, gzip, and pigz for maximum speed

Supports streaming backups to:
- Local filesystem
- AWS S3 (s3://bucket/path or --cloud s3)
- Google Drive (gs://folder or --cloud gdrive)
- OneDrive (onedrive://folder or --cloud onedrive)

Supports optional AES-256-CBC encryption via crypto.py module
"""

import os
import sys
import argparse
import subprocess
import glob
import time
import io
from pathlib import Path
from urllib.parse import urlparse

# Try to import crypto module
try:
    import crypto
    HAS_CRYPTO = True
except ImportError:
    crypto = None
    HAS_CRYPTO = False

# Try to import config
try:
    import config
    HAS_CONFIG = True
except ImportError:
    config = None
    HAS_CONFIG = False

# Optional cloud imports (loaded on demand)
try:
    import boto3
    HAS_S3 = True
except ImportError:
    HAS_S3 = False

try:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
    HAS_GDRIVE = True
except ImportError:
    HAS_GDRIVE = False

try:
    from msal import PublicClientApplication
    import requests
    HAS_ONEDRIVE = True
except ImportError:
    HAS_ONEDRIVE = False

# Default exclusion patterns for problematic files
DEFAULT_EXCLUSIONS = [
    # System directories (relative patterns that work with tar -C)
    "Library/*",
    "System/*",
    ".Trash/*",
    ".cache/*",
    ".local/share/Trash/*",
    "applications/*",
    "OneDrive-Comcast",
    "OneDrive-Comcast/*",
    "Comcast/*",
    
    # Dart/Flutter build artifacts
    "*.dill",
    "*.snapshot",
    ".dart_tool/flutter_build/*",
    ".dart_tool/chrome-device/*", 
    "build/flutter_assets/*",
    "build/web/*",
    
    # # General build artifacts  
    "node_modules/*",
    ".DS_Store",
    "*.tmp",
    "*.temp",
    "*.log",
    
    # Cache directories
    "__pycache__/*",
    "cache/*",
    "tmp/*",
    
    # # IDE files
    # ".vscode/*",
    # ".idea/*",
    # "*.swp",
    # "*.swo",
    
    # Large binary files that may be problematic
    # "*.iso",
    "*.dmg",
    # "*.img"
]

def run_command(cmd, capture_output=True, shell=True, check=True):
    """Run a bash command with proper error handling"""
    try:
        if isinstance(cmd, list):
            shell = False
        
        # Print command before execution
        print(f"🔧 Executing command:")
        if isinstance(cmd, list):
            print(f"   {' '.join(cmd)}")
        else:
            print(f"   {cmd}")
        print()
        
        result = subprocess.run(
            cmd, 
            shell=shell, 
            capture_output=capture_output, 
            text=True, 
            check=check
        )
        
        # Print result info
        if result.returncode == 0:
            print(f"✅ Command completed successfully (exit code: {result.returncode})")
        else:
            print(f"⚠️  Command exited with code: {result.returncode}")
        
        if result.stdout and capture_output:
            print(f"📤 Output: {result.stdout[:200]}...")  # First 200 chars
        if result.stderr:
            print(f"⚠️  Stderr: {result.stderr[:200]}...")
        
        print()
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {cmd}")
        print(f"   Exit code: {e.returncode}")
        if e.stdout:
            print(f"   Stdout: {e.stdout}")
        if e.stderr:
            print(f"   Stderr: {e.stderr}")
        print()
        raise

def check_dependencies():
    """Check if required tools are available"""
    tools = ['tar', 'gzip']
    
    # Check for gtar (GNU tar) - required for proper macOS support
    try:
        subprocess.run(['which', 'gtar'], capture_output=True, check=True)
        tools.append('gtar')
    except subprocess.CalledProcessError:
        print("⚠️  GNU tar (gtar) not found!")
        print("   macOS BSD tar has issues with extended attributes.")
        print("   Please install GNU tar:")
        print()
        print("   brew install gnu-tar")
        print()
        print("   After installation, restart this script.")
        sys.exit(1)
    
    # Check for pigz (parallel gzip) - faster alternative
    try:
        subprocess.run(['which', 'pigz'], capture_output=True, check=True)
        tools.append('pigz')
        return 'pigz'  # Use pigz if available
    except subprocess.CalledProcessError:
        pass
    
    # Check for pbzip2 (parallel bzip2)
    try:
        subprocess.run(['which', 'pbzip2'], capture_output=True, check=True)
        tools.append('pbzip2')
    except subprocess.CalledProcessError:
        pass
        
    return 'gzip'  # Default to gzip

def get_compression_command(compressor, threads=None):
    """Get the appropriate compression command"""
    if threads is None:
        threads = os.cpu_count() or 4
    
    if compressor == 'pigz':
        return f"pigz -p {threads}"
    elif compressor == 'pbzip2':
        return f"pbzip2 -p{threads}"
    else:
        return "gzip"

def get_encryption_config(args):
    """
    Get encryption configuration from args or config file
    
    Returns:
        tuple: (enabled, password, salt_hex, iterations)
    """
    # Check if encryption is enabled
    enabled = getattr(args, 'encrypt', False)
    if not enabled and HAS_CONFIG:
        enabled = getattr(config, 'ENCRYPTION_ENABLED', False)
    
    if not enabled:
        return False, None, None, 100000
    
    # Check if crypto module is available
    if not HAS_CRYPTO:
        print("❌ Crypto module not found! Encryption requires crypto.py")
        print("   Make sure crypto.py is in the same directory")
        sys.exit(1)
    
    # Check OpenSSL
    if not crypto.check_openssl():
        print("❌ OpenSSL not found! Encryption requires OpenSSL")
        print("   Install with: brew install openssl")
        sys.exit(1)
    
    # Get password
    password = getattr(args, 'password', None)
    if not password and HAS_CONFIG:
        password = getattr(config, 'ENCRYPTION_PASSWORD', None)
    if not password:
        # Prompt interactively
        try:
            password = crypto.get_password("🔐 Enter encryption password: ", confirm=True)
        except ValueError as e:
            print(f"❌ {e}")
            sys.exit(1)
    
    # Get salt
    salt_hex = getattr(args, 'salt', None)
    if not salt_hex and HAS_CONFIG:
        salt_hex = getattr(config, 'ENCRYPTION_SALT', None)
    
    # Get iterations
    iterations = getattr(args, 'iterations', None)
    if not iterations and HAS_CONFIG:
        iterations = getattr(config, 'ENCRYPTION_ITERATIONS', 100000)
    if not iterations:
        iterations = 100000
    
    return enabled, password, salt_hex, iterations

def create_exclusion_file(exclusions, temp_dir="."):
    """Create a temporary file with exclusion patterns for tar --exclude-from"""
    import tempfile
    
    # Use current directory instead of system temp
    fd, temp_file = tempfile.mkstemp(
        dir=temp_dir, 
        prefix="archivedir_exclude_", 
        suffix=".txt",
        text=True  # Text mode for better efficiency
    )
    
    try:
        # Write all patterns at once to minimize I/O operations
        with os.fdopen(fd, 'w', buffering=8192) as f:  # 8KB buffer
            f.write('\n'.join(exclusions) + '\n')
        return temp_file
    except:
        try:
            os.close(fd)
        except:
            pass
        raise

def should_exclude_path(path, exclusions, base_path):
    """Check if a path matches any exclusion pattern"""
    try:
        # Get relative path from base
        rel_path = os.path.relpath(path, base_path)
        
        for pattern in exclusions:
            # Simple glob-style matching
            if pattern.endswith('/*'):
                # Directory wildcard: Library/*
                prefix = pattern[:-2]
                if rel_path.startswith(prefix + '/') or rel_path == prefix:
                    return True
            elif '*' in pattern:
                # Wildcard pattern: *.dill
                import fnmatch
                if fnmatch.fnmatch(os.path.basename(path), pattern):
                    return True
                if fnmatch.fnmatch(rel_path, pattern):
                    return True
            else:
                # Exact match
                if rel_path == pattern or rel_path.startswith(pattern + '/'):
                    return True
        return False
    except:
        return False

def detect_cloud_destination(dest):
    """Detect if destination is cloud storage based on path or scheme"""
    if dest.startswith('s3://'):
        return 's3', dest[5:]  # Remove s3:// prefix
    elif dest.startswith('gs://'):
        return 'gdrive', dest[5:]  # Remove gs:// prefix
    elif dest.startswith('onedrive://'):
        return 'onedrive', dest[11:]  # Remove onedrive:// prefix
    else:
        return 'local', dest

def stream_to_s3(file_stream, bucket, key, part_size_mb=100):
    """Stream data to S3 using multipart upload"""
    if not HAS_S3:
        raise ImportError("boto3 not installed. Install with: pip install boto3")
    
    print(f"☁️  Streaming to S3: s3://{bucket}/{key}")
    
    s3_client = boto3.client('s3')
    
    # Initiate multipart upload
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu['UploadId']
    
    parts = []
    part_num = 1
    chunk_size = part_size_mb * 1024 * 1024
    
    try:
        while True:
            chunk = file_stream.read(chunk_size)
            if not chunk:
                break
            
            print(f"   📤 Uploading part {part_num} ({len(chunk) / (1024**2):.1f} MB)...")
            
            response = s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=chunk
            )
            
            parts.append({
                'PartNumber': part_num,
                'ETag': response['ETag']
            })
            
            part_num += 1
        
        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"   ✅ Successfully uploaded to S3: {len(parts)} parts")
        return True
        
    except Exception as e:
        print(f"   ❌ Upload failed: {e}")
        # Abort multipart upload on failure
        s3_client.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )
        raise

def stream_to_gdrive(file_stream, folder_path, filename, part_size_gb=2):
    """Stream data to Google Drive with 2GB part splitting and immediate upload in background threads"""
    if not HAS_GDRIVE:
        raise ImportError("Google API client not installed. Install with: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
    
    print(f"☁️  Streaming to Google Drive: {filename}")
    print(f"   💡 Using {part_size_gb}GB parts with background upload")
    
    # Import gdrive_helper for authentication and upload
    try:
        from gdrive_helper import authenticate, get_or_create_folder_path, upload_file_streaming
    except ImportError:
        raise ImportError("gdrive_helper.py not found. Make sure it's in the same directory.")
    
    import threading
    import queue
    
    # Authenticate
    service = authenticate()
    
    # Get or create folder structure (with timestamp folder)
    folder_id = None
    if folder_path and folder_path.strip():
        folder_id = get_or_create_folder_path(service, folder_path)
    
    # Stream and split into parts
    part_size_bytes = int(part_size_gb * 1024 * 1024 * 1024)
    part_num = 0
    total_uploaded = 0
    
    # Remove extension from filename for parts
    base_filename = filename.replace('.tar.gz', '').replace('.tar.bz2', '')
    ext = filename.replace(base_filename, '')
    
    # Queue for upload threads
    upload_threads = []
    upload_results = queue.Queue()
    
    def upload_part_thread(part_buffer, part_filename, part_num, part_size):
        """Upload part in background thread"""
        try:
            # Each thread needs its own service instance (Google API client is not thread-safe)
            thread_service = authenticate()
            
            print(f"   🔄 [Thread {part_num}] Uploading {part_filename} in background...")
            file_id = upload_file_streaming(
                thread_service,
                part_buffer,
                part_filename,
                folder_id=folder_id,
                mime_type='application/gzip',
                chunk_size_mb=10
            )
            upload_results.put({'part': part_num, 'success': True, 'size': part_size, 'file_id': file_id})
            print(f"   ✅ [Thread {part_num}] Upload complete: {part_filename}")
        except Exception as e:
            upload_results.put({'part': part_num, 'success': False, 'error': str(e)})
            print(f"   ❌ [Thread {part_num}] Upload failed: {e}")
        finally:
            part_buffer.close()
    
    while True:
        # Read one part into memory
        print(f"\n   📥 Reading part {part_num}...")
        buffer = io.BytesIO()
        bytes_read = 0
        
        while bytes_read < part_size_bytes:
            chunk = file_stream.read(min(1024 * 1024, part_size_bytes - bytes_read))  # 1MB chunks
            if not chunk:
                break
            buffer.write(chunk)
            bytes_read += len(chunk)
            
            if bytes_read % (100 * 1024 * 1024) == 0:  # Progress every 100MB
                print(f"   📊 Read {bytes_read / (1024**2):.0f} MB for part {part_num}...", end='\r')
        
        if bytes_read == 0:
            break  # No more data
        
        print(f"\n   ✅ Part {part_num} buffered: {bytes_read / (1024**2):.1f} MB")
        buffer.seek(0)  # Reset to beginning for upload
        
        # Start upload in background thread
        part_filename = f"{base_filename}.part_{part_num:03d}{ext}"
        
        upload_thread = threading.Thread(
            target=upload_part_thread,
            args=(buffer, part_filename, part_num, bytes_read),
            daemon=False
        )
        upload_thread.start()
        upload_threads.append(upload_thread)
        
        print(f"   🚀 Part {part_num} upload started in background thread")
        print(f"   💡 Main thread continues reading next part...")
        
        total_uploaded += bytes_read
        part_num += 1
        
        # Check if we read less than part size (last part)
        if bytes_read < part_size_bytes:
            break
    
    # Wait for all uploads to complete
    print(f"\n   ⏳ Waiting for all {len(upload_threads)} background uploads to complete...")
    for idx, thread in enumerate(upload_threads):
        thread.join()
        print(f"   ✅ Thread {idx} finished")
    
    # Check results
    failed_parts = []
    while not upload_results.empty():
        result = upload_results.get()
        if not result['success']:
            failed_parts.append(result['part'])
    
    if failed_parts:
        print(f"\n   ❌ Upload failed for parts: {failed_parts}")
        raise Exception(f"Upload failed for {len(failed_parts)} part(s)")
    
    print(f"\n   🎉 All {part_num} parts uploaded successfully!")
    print(f"   📊 Total size: {total_uploaded / (1024**3):.2f} GB")
    
    return part_num

def stream_to_onedrive(file_stream, folder_path, filename):
    """Stream data to OneDrive using upload session"""
    if not HAS_ONEDRIVE:
        raise ImportError("Microsoft Graph not installed. Install with: pip install msal requests")
    
    print(f"☁️  Streaming to OneDrive: {folder_path}/{filename}")
    
    # Load app configuration
    import json
    with open('onedrive_config.json') as f:
        config = json.load(f)
    
    # Get access token using MSAL
    app = PublicClientApplication(
        config['client_id'],
        authority=f"https://login.microsoftonline.com/{config['tenant_id']}"
    )
    
    result = app.acquire_token_interactive(scopes=["Files.ReadWrite.All"])
    access_token = result['access_token']
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Create upload session
    upload_url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{folder_path}/{filename}:/createUploadSession"
    session_response = requests.post(upload_url, headers=headers)
    upload_session = session_response.json()
    
    # Upload in chunks
    chunk_size = 320 * 1024 * 10  # 3.2 MB (must be multiple of 320 KB)
    upload_url = upload_session['uploadUrl']
    
    file_stream.seek(0, 2)  # Seek to end
    file_size = file_stream.tell()
    file_stream.seek(0)  # Seek back to start
    
    offset = 0
    while True:
        chunk = file_stream.read(chunk_size)
        if not chunk:
            break
        
        chunk_len = len(chunk)
        headers = {
            'Content-Length': str(chunk_len),
            'Content-Range': f'bytes {offset}-{offset + chunk_len - 1}/{file_size}'
        }
        
        print(f"   📤 Uploading bytes {offset}-{offset + chunk_len - 1}/{file_size}")
        
        response = requests.put(upload_url, headers=headers, data=chunk)
        offset += chunk_len
    
    print(f"   ✅ Successfully uploaded to OneDrive")
    return True

def should_exclude_path(path, exclusions, base_path):
    """Check if a path matches any exclusion pattern"""
    try:
        # Get relative path from base
        rel_path = os.path.relpath(path, base_path)
        
        for pattern in exclusions:
            # Simple glob-style matching
            if pattern.endswith('/*'):
                # Directory wildcard: Library/*
                prefix = pattern[:-2]
                if rel_path.startswith(prefix + '/') or rel_path == prefix:
                    return True
            elif '*' in pattern:
                # Wildcard pattern: *.dill
                import fnmatch
                if fnmatch.fnmatch(os.path.basename(path), pattern):
                    return True
                if fnmatch.fnmatch(rel_path, pattern):
                    return True
            else:
                # Exact match
                if rel_path == pattern or rel_path.startswith(pattern + '/'):
                    return True
        return False
    except:
        return False

def fast_backup(args):
    """Fast backup using native tar command"""
    source = args.source
    dest_dir = args.dest
    size_gb = getattr(args, 'size', None)
    
    print(f"\n🔍 Stage 1: Validating inputs...")
    print(f"   Source: {source}")
    print(f"   Destination: {dest_dir}")
    if size_gb:
        print(f"   Split size: {size_gb} GB per part")
    
    # Detect cloud destination
    cloud_type, cloud_path = detect_cloud_destination(dest_dir)
    if cloud_type != 'local':
        print(f"   ☁️  Cloud destination detected: {cloud_type}")
        print(f"   ☁️  Cloud path: {cloud_path}")
    
    # Validate inputs
    if not os.path.exists(source):
        print(f"❌ Source directory not found: {source}")
        return
    
    # Prepare exclusions early for size calculation
    print(f"\n🚫 Stage 2: Preparing exclusions...")
    exclusions = DEFAULT_EXCLUSIONS.copy()
    if hasattr(args, 'exclude') and args.exclude:
        exclusions.extend(args.exclude)
    print(f"   Total exclusion patterns: {len(exclusions)}")
    
    # Calculate source size with exclusions applied
    print(f"\n📊 Stage 3: Calculating source size (applying exclusions on-the-fly)...")
    print(f"   Scanning directory structure...")
    source_size = 0
    file_count = 0
    dir_count = 0
    excluded_count = 0
    
    # Get absolute base path for exclusion matching
    base_path = os.path.abspath(source)
    
    # Lightweight scan - just count, apply exclusions on-the-fly
    for root, dirs, files in os.walk(source):
        # Filter out excluded directories to prevent walking into them
        dirs[:] = [d for d in dirs if not should_exclude_path(os.path.join(root, d), exclusions, base_path)]
        dir_count += len(dirs)
        
        for file in files:
            try:
                file_path = os.path.join(root, file)
                
                # Check if file should be excluded
                if should_exclude_path(file_path, exclusions, base_path):
                    excluded_count += 1
                    continue
                
                source_size += os.path.getsize(file_path)
                file_count += 1
                
                # Progress indicator every 1000 files to show activity
                if (file_count + excluded_count) % 1000 == 0:
                    print(f"   Scanned {file_count} files ({excluded_count} excluded), {dir_count} dirs...", end='\r')
            except:
                pass
    
    print(f"\n   Total directories: {dir_count}")
    print(f"   Total files (included): {file_count}")
    print(f"   Total files (excluded): {excluded_count}")
    print(f"   Total size (after exclusions): {source_size / (1024**3):.2f} GB ({source_size / (1024**2):.1f} MB)")
    
    os.makedirs(dest_dir, exist_ok=True)
    
    # Determine compression method
    print(f"\n🔧 Stage 4: Setting up compression...")
    compressor = check_dependencies()
    comp_ext = ".gz" if compressor in ['gzip', 'pigz'] else ".bz2"
    print(f"   Compressor: {compressor}")
    print(f"   Threads: {os.cpu_count()}")
    print(f"   Extension: {comp_ext}")
    
    # Create exclusion file
    print(f"\n🚫 Stage 5: Creating exclusion file...")
    exclude_file = None
    if exclusions and not getattr(args, 'include_problematic', False):
        print(f"   Writing exclusion patterns to temp file...")
        exclude_file = create_exclusion_file(exclusions)
        if getattr(args, 'verbose', False):
            print(f"   Exclusion file: {exclude_file}")
    
    try:
        source_name = os.path.basename(os.path.abspath(source))
        
        # Get encryption configuration
        encrypt_enabled, password, salt_hex, iterations = get_encryption_config(args)
        
        # Generate or use provided salt
        if encrypt_enabled:
            if salt_hex is None:
                salt_hex = crypto.generate_salt().hex()
            print(f"\n🔐 Encryption Configuration:")
            print(f"   Algorithm: AES-256-CBC")
            print(f"   KDF: PBKDF2-SHA256")
            print(f"   Iterations: {iterations}")
            print(f"   Salt: {salt_hex[:16]}...")
        
        # Add Unix timestamp prefix to archive name
        timestamp = int(time.time())
        timestamped_name = f"{timestamp}/{source_name}"
        
        # Add .enc extension if encrypted
        archive_ext = comp_ext
        if encrypt_enabled:
            archive_ext += ".enc"
        
        base_output = os.path.join(dest_dir, f"{timestamped_name}.tar{archive_ext}")
        
        print(f"\n📦 Stage 6: Creating backup archive...")
        print(f"   Timestamp: {timestamp}")
        print(f"   Archive name: {timestamped_name}.tar{archive_ext}")
        print(f"   Full path: {base_output}")
        if encrypt_enabled:
            print(f"   🔐 Encryption: ENABLED")
        
        start_time = time.time()
        
        if size_gb and size_gb > 0:
            # Multi-part backup
            size_bytes = int(size_gb * 1024 * 1024 * 1024)
            estimated_parts = int((source_size / size_bytes) + 1)
            
            print(f"\n🧩 Multi-part mode:")
            print(f"   Part size: {size_gb} GB ({size_bytes / (1024**2):.0f} MB)")
            print(f"   Estimated parts: ~{estimated_parts}")
            
            # Create tar command with exclusions (using GNU tar)
            tar_cmd_parts = ["gtar", "-cf", "-", "--no-xattrs", "--no-acls"]
            
            if exclude_file:
                tar_cmd_parts.extend(["--exclude-from", exclude_file])
            
            tar_cmd_parts.extend(["-C", os.path.dirname(os.path.abspath(source)), source_name])
            
            # Compression and splitting pipeline
            comp_cmd = get_compression_command(compressor)
            
            if cloud_type != 'local':
                # Cloud streaming mode with configurable parts
                print(f"\n⏳ Running backup pipeline with cloud streaming...")
                print(f"   Command: tar → {compressor} → {cloud_type} ({size_gb}GB parts)")
                print(f"   💡 Streaming in {size_gb}GB parts - immediate upload and cleanup")
                print(f"   🔧 Using --no-xattrs, --no-acls to suppress warnings")
                
                # Create timestamp folder for this backup run
                backup_timestamp = int(time.time())
                cloud_folder = f"{cloud_path}/{backup_timestamp}" if cloud_path else str(backup_timestamp)
                
                print(f"   📁 Backup folder: {cloud_folder}")
                
                # Start tar+gzip pipeline
                tar_proc = subprocess.Popen(tar_cmd_parts, stdout=subprocess.PIPE)
                gzip_proc = subprocess.Popen(
                    comp_cmd.split(),
                    stdin=tar_proc.stdout,
                    stdout=subprocess.PIPE
                )
                
                # Stream to cloud with 2GB splits
                base_filename = f"{os.path.basename(source)}.tar{comp_ext}"
                
                if cloud_type == 's3':
                    parts = cloud_path.split('/', 1)
                    bucket = parts[0]
                    s3_folder = f"{parts[1]}/{backup_timestamp}" if len(parts) > 1 else str(backup_timestamp)
                    stream_to_s3(gzip_proc.stdout, bucket, s3_folder, base_filename, part_size_gb=size_gb)
                elif cloud_type == 'gdrive':
                    stream_to_gdrive(gzip_proc.stdout, cloud_folder, base_filename, part_size_gb=size_gb)
                elif cloud_type == 'onedrive':
                    stream_to_onedrive(gzip_proc.stdout, cloud_folder, base_filename, part_size_gb=size_gb)
                
                gzip_proc.wait()
                tar_proc.wait()
                
                print(f"\n   ✅ Cloud backup complete to {cloud_type}:{cloud_folder}")
                print(f"   📋 To extract, use:")
                if cloud_type == 'gdrive':
                    print(f"      python archivedir_fast.py extract gs://{cloud_folder}/{base_filename}.part_** ./output")
                elif cloud_type == 's3':
                    print(f"      python archivedir_fast.py extract s3://{bucket}/{s3_folder}/{base_filename}.part_** ./output")
                elif cloud_type == 'onedrive':
                    print(f"      python archivedir_fast.py extract onedrive://{cloud_folder}/{base_filename}.part_** ./output")
            else:
                # Local filesystem mode
                split_cmd = f"split -b {size_bytes} - \"{base_output}.part_\""
                
                # Build pipeline with optional encryption
                if encrypt_enabled:
                    # Save encryption metadata
                    crypto.save_metadata(base_output.replace(comp_ext + '.enc', ''), salt_hex, iterations)
                    
                    # Pipeline: tar → compress → encrypt → split
                    encrypt_cmd = crypto.encrypt_pipeline_cmd(password, salt_hex, iterations)
                    pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} | {encrypt_cmd} | {split_cmd}"
                else:
                    # Pipeline: tar → compress → split
                    pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} | {split_cmd}"
                
                print(f"\n⏳ Running backup pipeline...")
                if encrypt_enabled:
                    print(f"   Command: tar → {compressor} → encrypt → split")
                else:
                    print(f"   Command: tar → {compressor} → split")
                print(f"   Split pattern: {base_output}.part_*")
                print(f"   💡 Using streaming pipeline (minimal RAM/disk usage)")
                if encrypt_enabled:
                    print(f"   💡 Data flows: disk → tar → compress → encrypt → split → disk")
                else:
                    print(f"   💡 Data flows directly: disk → tar → compress → split → disk")
                print(f"   🔧 Using --no-xattrs, --no-acls to suppress warnings")
                
                # Run with minimal buffering for memory efficiency
                run_command(pipeline, capture_output=False)
            
            # Find all parts and report
            print(f"\n📊 Stage 7: Analyzing backup results...")
            print(f"   Scanning for created parts...")
            parts = sorted(glob.glob(f"{base_output}.part_*"))
            
            # Calculate total size efficiently without loading files
            total_size = sum(os.path.getsize(part) for part in parts)
            
            print(f"\n✅ Backup Complete!")
            print(f"   Parts created: {len(parts)}")
            print(f"   Total compressed size: {total_size / (1024**3):.2f} GB")
            if source_size > 0:
                print(f"   Compression ratio: {(total_size / source_size * 100):.1f}%")
                print(f"   Space saved: {((source_size - total_size) / (1024**3)):.2f} GB")
            print(f"   Output pattern: {base_output}.part_**")
            
            # Only list first 10 and last 5 parts to avoid huge output
            if len(parts) > 15:
                print(f"\n   Part list (showing first 10 and last 5):")
                for i, part in enumerate(parts[:10], 1):
                    part_size = os.path.getsize(part) / (1024**2)
                    print(f"      [{i}] {os.path.basename(part)} - {part_size:.1f} MB")
                print(f"      ... ({len(parts) - 15} more parts) ...")
                for i, part in enumerate(parts[-5:], len(parts) - 4):
                    part_size = os.path.getsize(part) / (1024**2)
                    print(f"      [{i}] {os.path.basename(part)} - {part_size:.1f} MB")
            else:
                print(f"\n   Part list:")
                for i, part in enumerate(parts, 1):
                    part_size = os.path.getsize(part) / (1024**2)
                    print(f"      [{i}] {os.path.basename(part)} - {part_size:.1f} MB")
            
        else:
            # Single file backup
            print(f"\n📦 Single file mode:")
            
            tar_cmd_parts = ["gtar", "-cf", "-", "--no-xattrs", "--no-acls"]
            
            if exclude_file:
                tar_cmd_parts.extend(["--exclude-from", exclude_file])
            
            tar_cmd_parts.extend(["-C", os.path.dirname(os.path.abspath(source)), source_name])
            
            # Build pipeline with optional encryption
            comp_cmd = get_compression_command(compressor)
            
            if encrypt_enabled:
                # Save encryption metadata
                crypto.save_metadata(base_output.replace(comp_ext + '.enc', ''), salt_hex, iterations)
                
                # Pipeline: tar → compress → encrypt
                encrypt_cmd = crypto.encrypt_pipeline_cmd(password, salt_hex, iterations)
                pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} | {encrypt_cmd} > \"{base_output}\""
            else:
                # Pipeline: tar → compress
                pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} > \"{base_output}\""
            
            print(f"\n⏳ Running backup pipeline...")
            if encrypt_enabled:
                print(f"   Command: tar → {compressor} → encrypt")
                print(f"   💡 Using streaming pipeline (minimal RAM/disk usage)")
                print(f"   💡 Data flows: disk → tar → compress → encrypt → disk")
            else:
                print(f"   Command: tar → {compressor}")
                print(f"   💡 Using streaming pipeline (minimal RAM/disk usage)")
                print(f"   💡 Data flows directly: disk → tar → compress → disk")
            print(f"   🔧 Using --no-xattrs, --no-acls to suppress warnings")
            run_command(pipeline, capture_output=False)
            
            print(f"\n📊 Stage 7: Analyzing backup results...")
            file_size = os.path.getsize(base_output)
            print(f"\n✅ Backup Complete!")
            print(f"   Output file: {os.path.basename(base_output)}")
            print(f"   Compressed size: {file_size / (1024**3):.2f} GB")
            if source_size > 0:
                print(f"   Compression ratio: {(file_size / source_size * 100):.1f}%")
                print(f"   Space saved: {((source_size - file_size) / (1024**3)):.2f} GB")
            print(f"   Full path: {base_output}")
        
        elapsed = time.time() - start_time
        print(f"⏱️  Completed in {elapsed:.1f} seconds")
        
    finally:
        # Clean up exclusion file
        if exclude_file and os.path.exists(exclude_file):
            os.unlink(exclude_file)

def check_and_download_onedrive_files(files):
    """Check if files are OneDrive offline files and trigger download"""
    print(f"   Checking {len(files)} files for OneDrive status...")
    onedrive_files = []
    
    for idx, file_path in enumerate(files, 1):
        try:
            print(f"   [{idx}/{len(files)}] Checking: {os.path.basename(file_path)}", end="")
            
            # Check if file is in OneDrive directory
            if "OneDrive" in file_path or "OneDrive-Comcast" in file_path:
                print(f" - OneDrive file", end="")
                # Check if file exists but might be offline (placeholder)
                file_size = os.path.getsize(file_path)
                print(f" - Size: {file_size / (1024**2):.1f} MB", end="")
                
                # OneDrive offline files are typically very small placeholders
                # Try to access the file to trigger download
                if file_size < 1024:  # Less than 1KB might be a placeholder
                    print(f" - ⚠️  OFFLINE PLACEHOLDER!")
                    onedrive_files.append(file_path)
                else:
                    print(f" - ✓ Available")
                    
                # Attempt to trigger download by reading file attributes
                try:
                    with open(file_path, 'rb') as f:
                        f.read(1)  # Read first byte to trigger download
                except Exception as read_err:
                    print(f" - ⚠️  Read error: {read_err}")
            else:
                print(f" - ✓ Local file")
        except Exception as e:
            print(f" - ⚠️  Error: {e}")
    
    if onedrive_files:
        print(f"\n   📥 Found {len(onedrive_files)} OneDrive offline file(s)")
        print(f"   💡 Triggering download and waiting for sync...")
        
        # Trigger download and wait for files to become available
        max_wait_time = 300  # 5 minutes maximum wait
        check_interval = 2   # Check every 2 seconds
        
        for idx, file_path in enumerate(onedrive_files, 1):
            try:
                print(f"\n   [{idx}/{len(onedrive_files)}] Processing: {os.path.basename(file_path)}")
                
                # Trigger download
                print(f"      🔄 Triggering download...")
                subprocess.run(['cat', file_path], capture_output=True, timeout=5)
                
                # Wait for file to become available (size > 1KB)
                start_wait = time.time()
                waited = 0
                
                while waited < max_wait_time:
                    current_size = os.path.getsize(file_path)
                    
                    if current_size >= 1024:
                        # File is now available
                        print(f"      ✅ Downloaded! Size: {current_size / (1024**2):.1f} MB (waited {waited:.0f}s)")
                        break
                    else:
                        # Still a placeholder, keep waiting
                        if waited == 0:
                            print(f"      ⏳ Waiting for OneDrive sync (checking every {check_interval}s)...", end="")
                        else:
                            print(f"\r      ⏳ Waiting... {waited:.0f}s elapsed (size: {current_size} bytes)", end="")
                        
                        time.sleep(check_interval)
                        waited = time.time() - start_wait
                
                # Check if we timed out
                if waited >= max_wait_time:
                    print(f"\r      ⚠️  Timeout after {max_wait_time}s - file may still be syncing")
                else:
                    print()  # New line after progress updates
                    
            except Exception as dl_err:
                print(f"      ⚠️  Error: {dl_err}")
        
        print(f"\n   ✅ OneDrive file check complete")
    else:
        print(f"   ✅ All files are local or already synced")

def fast_extract(args):
    """Fast extraction with optional decryption support and Google Drive streaming"""
    source_pattern = args.source
    dest = args.dest
    streaming = not getattr(args, 'no_streaming', False)
    
    # Check if source is a Google Drive folder ID or URL
    is_gdrive = False
    folder_id = None
    file_pattern = None
    
    if source_pattern.startswith('gs://') or source_pattern.startswith('gdrive://'):
        is_gdrive = True
        # Extract path and file pattern
        # Format: gs://folder_name/path/to/files/pattern or gs://folder_id/pattern
        path = source_pattern.replace('gs://', '').replace('gdrive://', '')
        
        # Split path into folder components and file pattern
        path_parts = path.split('/')
        folder_path = path_parts[:-1] if path_parts else []
        file_pattern = path_parts[-1] if path_parts else '*.tar.gz'
        
        print(f"☁️  Google Drive source detected")
        print(f"   Folder path: {'/'.join(folder_path)}")
        print(f"   File pattern: {file_pattern}")
    elif 'drive.google.com' in source_pattern:
        is_gdrive = True
        # Extract folder ID from URL
        # Format: https://drive.google.com/drive/folders/FOLDER_ID?...
        import re
        match = re.search(r'/folders/([a-zA-Z0-9_-]+)', source_pattern)
        if match:
            folder_id = match.group(1)
            file_pattern = '*.tar.gz'
            print(f"☁️  Google Drive URL detected")
            print(f"   Folder ID: {folder_id}")
            print(f"   File pattern: {file_pattern}")
        else:
            print(f"❌ Could not extract folder ID from URL: {source_pattern}")
            return
    
    # Handle different input patterns
    if not is_gdrive:
        if "*" not in source_pattern:
            if "part_" in source_pattern:
                base = source_pattern.split("part_")[0]
                source_pattern = base + "part_*"
            elif source_pattern.endswith((".tar.gz", ".tar.bz2", ".tar.gz.enc", ".tar.bz2.enc")):
                potential_part = source_pattern + ".part_aa"
                if os.path.exists(potential_part):
                    source_pattern = source_pattern + ".part_*"
    
    print(f"🔍 Looking for files matching: {source_pattern}")
    
    os.makedirs(dest, exist_ok=True)
    
    # Get file list - either from local or Google Drive
    files = []
    file_ids = {}  # Maps filename to file_id for Google Drive
    
    if is_gdrive:
        if not HAS_GDRIVE:
            print(f"❌ Google Drive support not available. Install with:")
            print(f"   pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
            return
        
        try:
            from gdrive_helper import authenticate
            service = authenticate()
            
            print(f"   📡 Navigating Google Drive folder structure...")
            
            # Navigate through folder hierarchy to find the target folder
            current_folder_id = 'root'
            
            for folder_name in folder_path:
                # Search for folder by name in current parent
                query = f"'{current_folder_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
                results = service.files().list(
                    q=query,
                    spaces='drive',
                    fields='files(id, name)',
                    pageSize=1
                ).execute()
                
                folders = results.get('files', [])
                if not folders:
                    print(f"❌ Folder not found: {folder_name}")
                    return
                
                current_folder_id = folders[0]['id']
                print(f"   ✓ Found folder: {folder_name} (ID: {current_folder_id})")
            
            folder_id = current_folder_id
            
            print(f"   📡 Listing files in folder...")
            
            # List all files in the target folder first
            query = f"'{folder_id}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'"
            
            results = service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, size)',
                orderBy='name',
                pageSize=1000
            ).execute()
            
            gdrive_files = results.get('files', [])
            
            if not gdrive_files:
                print(f"❌ No files found in folder")
                return
            
            print(f"   📄 Found {len(gdrive_files)} files in folder:")
            for gfile in gdrive_files[:10]:  # Show first 10
                size_mb = int(gfile.get('size', 0)) / (1024**2)
                print(f"      • {gfile['name']} ({size_mb:.1f} MB)")
            if len(gdrive_files) > 10:
                print(f"      ... and {len(gdrive_files) - 10} more files")
            
            # Filter and sort files using pattern matching
            import fnmatch
            # Normalize pattern: ** means any characters
            normalized_pattern = file_pattern.replace('**', '*')
            
            # Try direct match first
            for gfile in gdrive_files:
                if fnmatch.fnmatch(gfile['name'], normalized_pattern):
                    files.append(gfile['name'])
                    file_ids[gfile['name']] = gfile['id']
            
            # If no matches and pattern has .tar.gz.part_, try swapping to .part_.tar.gz
            if not files and '.tar.gz.part_' in normalized_pattern:
                alt_pattern = normalized_pattern.replace('.tar.gz.part_', '.part_*.tar.gz')
                print(f"   💡 Trying alternative pattern: {alt_pattern}")
                for gfile in gdrive_files:
                    if fnmatch.fnmatch(gfile['name'], alt_pattern):
                        files.append(gfile['name'])
                        file_ids[gfile['name']] = gfile['id']
            
            # Also try .part_*.tar.bz2 for bzip2 archives
            if not files and '.tar.bz2.part_' in normalized_pattern:
                alt_pattern = normalized_pattern.replace('.tar.bz2.part_', '.part_*.tar.bz2')
                print(f"   💡 Trying alternative pattern: {alt_pattern}")
                for gfile in gdrive_files:
                    if fnmatch.fnmatch(gfile['name'], alt_pattern):
                        files.append(gfile['name'])
                        file_ids[gfile['name']] = gfile['id']
            
            files = sorted(files)
            
            if not files:
                print(f"   ⚠️  No files matched pattern: {file_pattern}")
                print(f"   💡 Sample filename: {gdrive_files[0]['name'] if gdrive_files else 'N/A'}")
                print(f"   💡 Try using: *.part_*.tar.gz")
                return
            
        except Exception as e:
            print(f"❌ Error accessing Google Drive: {e}")
            return
    else:
        files = sorted(glob.glob(source_pattern))
    
    if not files:
        print(f"❌ No files found matching pattern: {source_pattern}")
        return
    
    print(f"✅ Found {len(files)} file(s) to extract")
    
    # Show file list with sizes
    if is_gdrive:
        for i, fname in enumerate(files, 1):
            print(f"   [{i}] {fname}")
    else:
        for i, f in enumerate(files, 1):
            print(f"   [{i}] {os.path.basename(f)} ({os.path.getsize(f) / (1024**2):.1f} MB)")
    
    # Check for encryption
    encrypted = False
    password = None
    salt_hex = None
    iterations = 100000
    
    # Check if this is an encrypted archive
    base_file = files[0]
    if base_file.endswith('.enc'):
        encrypted = True
        print(f"\n🔐 Encrypted archive detected")
        
        if not HAS_CRYPTO:
            print(f"   ❌ crypto.py module not available")
            return
        
        # Try to load metadata from .enc file (skip for Google Drive)
        if not is_gdrive:
            metadata_base = base_file.replace('.enc', '').replace('.part_aa', '').replace('.tar.gz', '').replace('.tar.bz2', '')
            try:
                metadata = crypto.load_metadata(metadata_base)
                if metadata:
                    salt_hex = metadata.get('salt')
                    iterations = metadata.get('iterations', 100000)
                    print(f"   ✓ Loaded encryption metadata (iterations: {iterations})")
            except Exception as e:
                print(f"   ⚠️  Could not load metadata: {e}")
        
        # Get password from args or prompt
        password = getattr(args, 'password', None)
        if not password:
            password = crypto.get_password(confirm=False)
        
        # Get salt from args if provided
        if hasattr(args, 'salt') and args.salt:
            salt_hex = args.salt
        
        if not salt_hex and not is_gdrive:
            print(f"   ❌ Salt not found in metadata and not provided via --salt")
            return
        
        if salt_hex:
            print(f"   ✓ Decryption configured")
    
    # Check and download OneDrive offline files if needed (only for local files)
    if not is_gdrive:
        print(f"\n🔍 Stage 1: Checking OneDrive status...")
        check_and_download_onedrive_files(files)
    
    print(f"\n🧩 Stage 2: Starting extraction from: {source_pattern if not is_gdrive else f'Google Drive folder {folder_id}'}")
    
    # Determine decompression method
    base_name = files[0].replace('.enc', '')
    if base_name.endswith('.bz2'):
        decomp_cmd = "pbzip2 -dc" if subprocess.run(['which', 'pbzip2'], capture_output=True).returncode == 0 else "bzip2 -dc"
    else:
        decomp_cmd = "pigz -dc" if subprocess.run(['which', 'pigz'], capture_output=True).returncode == 0 else "gzip -dc"
    
    start_time = time.time()
    
    # Google Drive streaming extraction
    if is_gdrive:
        print(f"☁️  Google Drive streaming mode")
        print(f"📦 Multi-part archive: {len(files)} parts")
        print(f"💡 Will download → extract → delete each part in sequence")
        
        if encrypted:
            print(f"🔧 Pipeline: download → decrypt → {decomp_cmd.split()[0]} → extract → delete")
        else:
            print(f"🔧 Pipeline: download → {decomp_cmd.split()[0]} → extract → delete")
        
        print(f"📦 Extracting to: {dest}\n")
        
        # Use named pipe (FIFO) for streaming extraction
        import tempfile
        import threading
        fifo_path = os.path.join(tempfile.gettempdir(), f"archivedir_fifo_{os.getpid()}")
        
        # Create FIFO
        if os.path.exists(fifo_path):
            os.unlink(fifo_path)
        os.mkfifo(fifo_path)
        
        try:
            # Start tar extraction in background
            def extract_from_fifo():
                if encrypted:
                    decrypt_cmd = crypto.decrypt_pipeline_cmd(password, salt_hex, iterations)
                    extract_cmd = f"{decrypt_cmd} < \"{fifo_path}\" | {decomp_cmd} | tar -xf - -C \"{dest}\""
                else:
                    extract_cmd = f"{decomp_cmd} < \"{fifo_path}\" | tar -xf - -C \"{dest}\""
                subprocess.run(extract_cmd, shell=True, check=False)
            
            extractor = threading.Thread(target=extract_from_fifo, daemon=True)
            extractor.start()
            
            # Track bytes written to FIFO
            bytes_in_fifo = 0
            fifo_max_size = 3 * 1024**3  # 3GB limit
            lock = threading.Lock()
            
            # Monitor extraction progress to track FIFO consumption
            def monitor_extraction():
                nonlocal bytes_in_fifo
                last_check = time.time()
                while extractor.is_alive():
                    time.sleep(1)
                    # Estimate bytes consumed (FIFO drains as tar extracts)
                    # This is approximate - FIFO size reduces as data is read
                    with lock:
                        # Decay the counter over time as extraction progresses
                        elapsed = time.time() - last_check
                        if elapsed > 0:
                            bytes_in_fifo = max(0, bytes_in_fifo - int(50 * 1024**2 * elapsed))  # Assume ~50MB/s extraction
                        last_check = time.time()
            
            monitor = threading.Thread(target=monitor_extraction, daemon=True)
            monitor.start()
            
            # Download and stream each part through the FIFO
            with open(fifo_path, 'wb') as fifo:
                for i, filename in enumerate(files):
                    file_id = file_ids[filename]
                    
                    print(f"📥 [{i+1}/{len(files)}] Downloading: {filename}")
                    
                    # Download file directly and stream to FIFO
                    from googleapiclient.http import MediaIoBaseDownload
                    import io
                    request = service.files().get_media(fileId=file_id)
                    
                    # Use a buffer for streaming
                    buffer = io.BytesIO()
                    downloader = MediaIoBaseDownload(buffer, request, chunksize=10*1024*1024)  # 10MB chunks
                    
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            progress = int(status.progress() * 100)
                            print(f"   📊 Download progress: {progress}%", end='\r')
                        
                        # Write buffer to FIFO and reset
                        buffer.seek(0)
                        data = buffer.read()
                        if data:
                            # Wait if FIFO buffer is too large
                            while True:
                                with lock:
                                    if bytes_in_fifo < fifo_max_size:
                                        break
                                print(f"\n   ⏸️  FIFO buffer full ({bytes_in_fifo / (1024**3):.2f} GB), waiting for extraction...", end='\r')
                                time.sleep(0.5)
                            
                            fifo.write(data)
                            with lock:
                                bytes_in_fifo += len(data)
                            buffer.seek(0)
                            buffer.truncate(0)
                    
                    # Write any remaining data
                    buffer.seek(0)
                    remaining = buffer.read()
                    if remaining:
                        # Wait if FIFO buffer is too large
                        while True:
                            with lock:
                                if bytes_in_fifo < fifo_max_size:
                                    break
                            print(f"\n   ⏸️  FIFO buffer full ({bytes_in_fifo / (1024**3):.2f} GB), waiting for extraction...", end='\r')
                            time.sleep(0.5)
                        
                        fifo.write(remaining)
                        with lock:
                            bytes_in_fifo += len(remaining)
                    
                    print(f"\n   ✅ Downloaded and streamed: {filename}")
            
            # Wait for extraction to complete
            extractor.join(timeout=60)
            
        finally:
            if os.path.exists(fifo_path):
                os.unlink(fifo_path)
        
        print(f"\n   🎉 All parts extracted from Google Drive!")
        
    # Local file extraction
    else:
        try:
            if len(files) == 1:
                # Single file extraction
                print(f"📁 Single file detected: {os.path.basename(files[0])}")
                archive_size = os.path.getsize(files[0])
                print(f"📦 Archive size: {archive_size / (1024**3):.2f} GB")
                
                if encrypted:
                    print(f"🔧 Pipeline: decrypt → {decomp_cmd.split()[0]} → extract")
                    decrypt_cmd = crypto.decrypt_pipeline_cmd(password, salt_hex, iterations)
                    extract_cmd = f"{decrypt_cmd} < \"{files[0]}\" | {decomp_cmd} | tar -xf - -C \"{dest}\""
                else:
                    print(f"🔧 Decompression: {decomp_cmd}")
                    extract_cmd = f"{decomp_cmd} \"{files[0]}\" | tar -xf - -C \"{dest}\""
                
                print(f"📦 Extracting to: {dest}")
                print(f"⏳ Please wait...\n")
                
                run_command(extract_cmd, capture_output=False)
                
            else:
                # Multi-part extraction
                print(f"📦 Multi-part archive detected: {len(files)} parts")
                total_size = sum(os.path.getsize(f) for f in files)
                print(f"📦 Total compressed size: {total_size / (1024**3):.2f} GB")
                
                if encrypted:
                    print(f"🔧 Pipeline: concat → decrypt → {decomp_cmd.split()[0]} → extract")
                else:
                    print(f"🔧 Decompression: {decomp_cmd}")
                
                if streaming:
                    # Streaming mode: extract as parts become available
                    print(f"\n🌊 Streaming mode enabled: will extract parts serially")
                    print(f"💡 Parts will be processed one at a time")
                    print(f"📦 Extracting to: {dest}\n")
                    
                    # Use named pipe (FIFO) for streaming extraction
                    import tempfile
                    fifo_path = os.path.join(tempfile.gettempdir(), f"archivedir_fifo_{os.getpid()}")
                    
                    # Create FIFO
                    if os.path.exists(fifo_path):
                        os.unlink(fifo_path)
                    os.mkfifo(fifo_path)
                    
                    try:
                        # Start tar extraction in background
                        import threading
                        
                        def extract_from_fifo():
                            if encrypted:
                                decrypt_cmd = crypto.decrypt_pipeline_cmd(password, salt_hex, iterations)
                                extract_cmd = f"{decrypt_cmd} < \"{fifo_path}\" | {decomp_cmd} | tar -xf - -C \"{dest}\""
                            else:
                                extract_cmd = f"{decomp_cmd} < \"{fifo_path}\" | tar -xf - -C \"{dest}\""
                            subprocess.run(extract_cmd, shell=True, check=False)
                        
                        extractor = threading.Thread(target=extract_from_fifo, daemon=True)
                        extractor.start()
                        
                        # Feed parts into FIFO serially
                        with open(fifo_path, 'wb') as fifo:
                            for i, part_file in enumerate(files):
                                part_size = os.path.getsize(part_file) / (1024**2)
                                print(f"📥 [{i+1}/{len(files)}] Processing: {os.path.basename(part_file)} ({part_size:.1f} MB)")
                                with open(part_file, 'rb') as part:
                                    bytes_read = 0
                                    while chunk := part.read(1024 * 1024):  # 1MB chunks
                                        fifo.write(chunk)
                                        bytes_read += len(chunk)
                                print(f"   ✓ Completed {os.path.basename(part_file)}")
                        
                        extractor.join(timeout=60)
                        
                    finally:
                        if os.path.exists(fifo_path):
                            os.unlink(fifo_path)
                else:
                    # Standard mode: all parts must be present
                    print(f"\n⚡ Standard mode: concatenating all parts at once")
                    print(f"📦 Extracting to: {dest}")
                    print(f"⏳ Please wait...\n")
                    
                    # Concatenate and extract - keep directory structure
                    cat_cmd = " ".join([f'cat "{f}"' for f in files])
                    if encrypted:
                        decrypt_cmd = crypto.decrypt_pipeline_cmd(password, salt_hex, iterations)
                        extract_cmd = f"{cat_cmd} | {decrypt_cmd} | {decomp_cmd} | tar -xf - -C \"{dest}\""
                    else:
                        extract_cmd = f"{cat_cmd} | {decomp_cmd} | tar -xf - -C \"{dest}\""
                    
                    run_command(extract_cmd, capture_output=False)
            
            # Calculate extracted size
            print(f"\n📊 Stage 3: Calculating extraction results...")
            extracted_size = 0
            file_count = 0
            for root, dirs, files_list in os.walk(dest):
                for file in files_list:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        extracted_size += os.path.getsize(file_path)
                        file_count += 1
            
            # Auto-scale size units
            if extracted_size >= 1024**3:
                size_str = f"{extracted_size / (1024**3):.1f} GB"
            elif extracted_size >= 1024**2:
                size_str = f"{extracted_size / (1024**2):.1f} MB"
            else:
                size_str = f"{extracted_size / 1024:.1f} KB"
            
            elapsed = time.time() - start_time
            print(f"✅ Fast extraction complete: {file_count} files, {size_str}")
            print(f"⏱️  Completed in {elapsed:.1f} seconds")
            
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Extraction completed with some errors (likely due to archive corruption)")
            print(f"✅ Partial extraction may still be successful - check destination folder")

def main():
    parser = argparse.ArgumentParser(
        description="Fast Archive Directory Tool - Stream backups to local/cloud storage",
        epilog="Examples:\n"
               "  Local:        --dest /backup/folder\n"
               "  AWS S3:       --dest s3://bucket/path\n"
               "  Google Drive: --dest gs://folder_id\n"
               "  OneDrive:     --dest onedrive://path\n"
               "\nSee CLOUD_SETUP.md for detailed cloud configuration instructions.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create archive backup')
    backup_parser.add_argument('--source', required=True, help='Source directory to backup')
    backup_parser.add_argument('--dest', required=True, 
                              help='Destination: local path, s3://bucket/path, gs://folder_id, or onedrive://path')
    backup_parser.add_argument('--size', type=float, help='Split size in GB (creates multi-part archive)')
    backup_parser.add_argument('--exclude', action='append', help='Additional exclusion patterns')
    backup_parser.add_argument('--include-problematic', action='store_true', help='Include potentially problematic files')
    backup_parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    # Encryption options
    backup_parser.add_argument('--encrypt', action='store_true', help='Enable encryption (AES-256-CBC)')
    backup_parser.add_argument('--password', help='Encryption password (will prompt if not provided)')
    backup_parser.add_argument('--salt', help='Encryption salt as hex string (generates random if not provided)')
    backup_parser.add_argument('--iterations', type=int, default=100000, help='PBKDF2 iterations (default: 100000)')
    
    # Cloud-specific options
    backup_parser.add_argument('--cloud', choices=['s3', 'gdrive', 'onedrive'], 
                              help='Explicitly specify cloud provider (auto-detected from dest if using URL scheme)')
    backup_parser.add_argument('--aws-profile', default='default', 
                              help='AWS profile name (default: default)')
    backup_parser.add_argument('--gdrive-credentials', default='gdrive_credentials.json',
                              help='Google Drive credentials file (default: gdrive_credentials.json)')
    backup_parser.add_argument('--gdrive-token', default='gdrive_token.json',
                              help='Google Drive token file (default: gdrive_token.json)')
    backup_parser.add_argument('--onedrive-config', default='onedrive_config.json',
                              help='OneDrive configuration file (default: onedrive_config.json)')
    
    # Extract command
    extract_parser = subparsers.add_parser('extract', help='Extract archive')
    extract_parser.add_argument('--source', required=True, help='Source archive pattern')
    extract_parser.add_argument('--dest', required=True, help='Destination directory')
    extract_parser.add_argument('--no-streaming', action='store_true', 
                              help='Disable streaming mode (load all parts at once)')
    
    # Decryption options
    extract_parser.add_argument('--password', help='Decryption password (will prompt if archive is encrypted)')
    extract_parser.add_argument('--salt', help='Decryption salt as hex string (auto-loads from .enc metadata if available)')
    extract_parser.add_argument('--iterations', type=int, help='PBKDF2 iterations (auto-loads from .enc metadata if available)')
    extract_parser.add_argument('--keep-structure', action='store_true', help='Keep original directory structure instead of stripping top level')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'backup':
        fast_backup(args)
    elif args.command == 'extract':
        fast_extract(args)

if __name__ == '__main__':
    main()