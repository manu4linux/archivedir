#!/usr/bin/env python3
"""
Fast Archive Directory Tool - Using native bash commands for better performance
Leverages system tar, gzip, and pigz for maximum speed
"""

import os
import sys
import argparse
import subprocess
import glob
import time
from pathlib import Path

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
    # "Comcast",
    "Comcast/*",
    
    # Dart/Flutter build artifacts
    "*.dill",
    "*.snapshot",
    ".dart_tool/flutter_build/*",
    ".dart_tool/chrome-device/*", 
    "build/flutter_assets/*",
    "build/web/*",
    
    # # General build artifacts  
    # "node_modules/*",
    # # ".git/*",
    # ".DS_Store",
    # "*.tmp",
    # "*.temp",
    # "*.log",
    
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
    "*.iso",
    "*.dmg",
    "*.img"
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

def create_exclusion_file(exclusions, temp_dir="/tmp"):
    """Create a temporary file with exclusion patterns for tar --exclude-from"""
    import tempfile
    
    # Use system temp with automatic cleanup
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
        
        # Add Unix timestamp prefix to archive name
        timestamp = int(time.time())
        timestamped_name = f"{timestamp}_{source_name}"
        base_output = os.path.join(dest_dir, f"{timestamped_name}.tar{comp_ext}")
        
        print(f"\n📦 Stage 6: Creating backup archive...")
        print(f"   Timestamp: {timestamp}")
        print(f"   Archive name: {timestamped_name}.tar{comp_ext}")
        print(f"   Full path: {base_output}")
        
        start_time = time.time()
        
        if size_gb and size_gb > 0:
            # Multi-part backup
            size_bytes = int(size_gb * 1024 * 1024 * 1024)
            estimated_parts = int((source_size / size_bytes) + 1)
            
            print(f"\n🧩 Multi-part mode:")
            print(f"   Part size: {size_gb} GB ({size_bytes / (1024**2):.0f} MB)")
            print(f"   Estimated parts: ~{estimated_parts}")
            
            # Create tar command with exclusions
            tar_cmd_parts = ["tar", "-cf", "-", "--no-xattrs"]
            
            if exclude_file:
                tar_cmd_parts.extend(["--exclude-from", exclude_file])
            
            tar_cmd_parts.extend(["-C", os.path.dirname(os.path.abspath(source)), source_name])
            
            # Compression and splitting pipeline
            comp_cmd = get_compression_command(compressor)
            split_cmd = f"split -b {size_bytes} - \"{base_output}.part_\""
            
            # Full pipeline command
            pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} | {split_cmd}"
            
            print(f"\n⏳ Running backup pipeline...")
            print(f"   Command: tar → {compressor} → split")
            print(f"   Split pattern: {base_output}.part_*")
            print(f"   💡 Using streaming pipeline (minimal RAM/disk usage)")
            print(f"   💡 Data flows directly: disk → tar → compress → split → disk")
            print(f"   🔧 Using --no-xattrs to suppress extended attributes warnings")
            
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
            
            tar_cmd_parts = ["tar", "-cf", "-", "--no-xattrs"]
            
            if exclude_file:
                tar_cmd_parts.extend(["--exclude-from", exclude_file])
            
            tar_cmd_parts.extend(["-C", os.path.dirname(os.path.abspath(source)), source_name])
            
            # Compression pipeline
            comp_cmd = get_compression_command(compressor)
            pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} > \"{base_output}\""
            
            print(f"\n⏳ Running backup pipeline...")
            print(f"   Command: tar → {compressor}")
            print(f"   💡 Using streaming pipeline (minimal RAM/disk usage)")
            print(f"   💡 Data flows directly: disk → tar → compress → disk")
            print(f"   🔧 Using --no-xattrs to suppress extended attributes warnings")
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
        print(f"   💡 Triggering download...")
        
        # Use macOS xattr to check OneDrive status and trigger download
        for idx, file_path in enumerate(onedrive_files, 1):
            try:
                print(f"   [{idx}/{len(onedrive_files)}] Triggering: {os.path.basename(file_path)}")
                # Use 'cat' command to force OneDrive to download the file
                subprocess.run(['cat', file_path], capture_output=True, timeout=5)
                print(f"      ✓ Download triggered")
            except Exception as dl_err:
                print(f"      ⚠️  Failed: {dl_err}")
        
        # Give OneDrive time to start downloading
        print(f"   ⏳ Waiting 2 seconds for OneDrive sync...")
        import time
        time.sleep(2)
        
        print(f"   ✅ OneDrive file check complete")
    else:
        print(f"   ✅ All files are local or already synced")

def fast_extract(args):
    """Fast extraction using native tar command"""
    source_pattern = args.source
    dest = args.dest
    streaming = not getattr(args, 'no_streaming', False)
    
    # Handle different input patterns
    if "*" not in source_pattern:
        if "part_" in source_pattern:
            base = source_pattern.split("part_")[0]
            source_pattern = base + "part_*"
        elif source_pattern.endswith((".tar.gz", ".tar.bz2")):
            potential_part = source_pattern + ".part_aa"
            if os.path.exists(potential_part):
                source_pattern = source_pattern + ".part_*"
    
    print(f"🔍 Looking for files matching: {source_pattern}")
    
    os.makedirs(dest, exist_ok=True)
    
    files = sorted(glob.glob(source_pattern))
    if not files:
        print(f"❌ No files found matching pattern: {source_pattern}")
        return
    
    print(f"✅ Found {len(files)} file(s) to extract")
    for i, f in enumerate(files, 1):
        print(f"   [{i}] {os.path.basename(f)} ({os.path.getsize(f) / (1024**2):.1f} MB)")
    
    # Check and download OneDrive offline files if needed
    print(f"\n🔍 Stage 1: Checking OneDrive status...")
    check_and_download_onedrive_files(files)
    
    print(f"\n🧩 Stage 2: Starting extraction from: {source_pattern}")
    
    # Determine decompression method
    if files[0].endswith('.bz2'):
        decomp_cmd = "pbzip2 -dc" if subprocess.run(['which', 'pbzip2'], capture_output=True).returncode == 0 else "bzip2 -dc"
    else:
        decomp_cmd = "pigz -dc" if subprocess.run(['which', 'pigz'], capture_output=True).returncode == 0 else "gzip -dc"
    
    start_time = time.time()
    
    try:
        if len(files) == 1:
            # Single file extraction
            print(f"📁 Single file detected: {os.path.basename(files[0])}")
            archive_size = os.path.getsize(files[0])
            print(f"📦 Archive size: {archive_size / (1024**3):.2f} GB")
            print(f"🔧 Decompression: {decomp_cmd}")
            
            # Direct extraction with native tar - keep directory structure
            extract_cmd = f"{decomp_cmd} \"{files[0]}\" | tar -xf - -C \"{dest}\""
            print(f"📦 Extracting to: {dest}")
            print(f"⏳ Please wait...\n")
            
            run_command(extract_cmd, capture_output=False)
            
        else:
            # Multi-part extraction
            print(f"📦 Multi-part archive detected: {len(files)} parts")
            total_size = sum(os.path.getsize(f) for f in files)
            print(f"📦 Total compressed size: {total_size / (1024**3):.2f} GB")
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
    parser = argparse.ArgumentParser(description="Fast Archive Directory Tool using native commands")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create archive backup')
    backup_parser.add_argument('--source', required=True, help='Source directory to backup')
    backup_parser.add_argument('--dest', required=True, help='Destination directory for archive')
    backup_parser.add_argument('--size', type=float, help='Split size in GB (creates multi-part archive)')
    backup_parser.add_argument('--exclude', action='append', help='Additional exclusion patterns')
    backup_parser.add_argument('--include-problematic', action='store_true', help='Include potentially problematic files')
    backup_parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    # Extract command
    extract_parser = subparsers.add_parser('extract', help='Extract archive')
    extract_parser.add_argument('--source', required=True, help='Source archive pattern')
    extract_parser.add_argument('--dest', required=True, help='Destination directory')
    extract_parser.add_argument('--no-streaming', action='store_true', help='Disable streaming mode - wait for all parts before extraction (default: streaming enabled)')
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