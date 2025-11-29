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
    "/Users/mpadur210/Library/**",
    "/Users/mpadur210/Library/**",
    "/Users/mpadur210/System/**",
    "/Users/mpadur210/.Trash/**",
    "/Users/mpadur210/.cache/**",
    "/Users/mpadur210/.local/share/Trash/**",
    "/Users/mpadur210/applications/**",
    "/Users/mpadur210/OneDrive-Comcast",
    "/Users/mpadur210/Comcast",
    # # Dart/Flutter build artifacts
    # "*.dill",
    # "*.snapshot",
    # ".dart_tool/flutter_build/**",
    # ".dart_tool/chrome-device/**", 
    # "build/flutter_assets/**",
    # "build/web/**",
    
    # # General build artifacts  
    # "node_modules/**",
    # ".git/**",
    # "**/.DS_Store",
    # "*.tmp",
    # "*.temp",
    # "*.log",
    
    # # Cache directories
    # "**/__pycache__/**",
    # "**/cache/**",
    # "**/tmp/**",
    # ".cache/**",
    
    # # IDE files
    # ".vscode/**",
    # ".idea/**",
    # "*.swp",
    # "*.swo",
    
    # # Large binary files that may be problematic
    # "*.iso",
    # "*.dmg",
    # "*.img"
]

def run_command(cmd, capture_output=True, shell=True, check=True):
    """Run a bash command with proper error handling"""
    try:
        if isinstance(cmd, list):
            shell = False
        
        result = subprocess.run(
            cmd, 
            shell=shell, 
            capture_output=capture_output, 
            text=True, 
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {cmd}")
        print(f"Error: {e.stderr if e.stderr else e}")
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
    
    fd, temp_file = tempfile.mkstemp(dir=temp_dir, prefix="archivedir_exclude_", suffix=".txt")
    
    try:
        with os.fdopen(fd, 'w') as f:
            for pattern in exclusions:
                f.write(f"{pattern}\n")
        return temp_file
    except:
        os.close(fd)
        raise

def fast_backup(args):
    """Fast backup using native tar command"""
    source = args.source
    dest_dir = args.dest
    size_gb = getattr(args, 'size', None)
    
    # Validate inputs
    if not os.path.exists(source):
        print(f"❌ Source directory not found: {source}")
        return
    
    os.makedirs(dest_dir, exist_ok=True)
    
    # Determine compression method
    compressor = check_dependencies()
    comp_ext = ".gz" if compressor in ['gzip', 'pigz'] else ".bz2"
    
    # Prepare exclusions
    exclusions = DEFAULT_EXCLUSIONS.copy()
    if hasattr(args, 'exclude') and args.exclude:
        exclusions.extend(args.exclude)
    
    # Create exclusion file
    exclude_file = None
    if exclusions and not getattr(args, 'include_problematic', False):
        exclude_file = create_exclusion_file(exclusions)
        if getattr(args, 'verbose', False):
            print(f"📋 Auto-excluding {len(exclusions)} patterns")
    
    try:
        source_name = os.path.basename(os.path.abspath(source))
        base_output = os.path.join(dest_dir, f"{source_name}.tar{comp_ext}")
        
        print(f"🚀 Fast backup: {source} -> {base_output}")
        print(f"⚡ Using {compressor} compression with {os.cpu_count()} threads")
        
        start_time = time.time()
        
        if size_gb and size_gb > 0:
            # Multi-part backup
            size_bytes = int(size_gb * 1024 * 1024 * 1024)
            
            # Create tar command with exclusions
            tar_cmd_parts = ["tar", "-cf", "-"]
            
            if exclude_file:
                tar_cmd_parts.extend(["--exclude-from", exclude_file])
            
            tar_cmd_parts.extend(["-C", os.path.dirname(os.path.abspath(source)), source_name])
            
            # Compression and splitting pipeline
            comp_cmd = get_compression_command(compressor)
            split_cmd = f"split -b {size_bytes} - \"{base_output}.part_\""
            
            # Full pipeline command
            pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} | {split_cmd}"
            
            print(f"📦 Creating multi-part archive (max {size_gb} GB per part)...")
            run_command(pipeline, capture_output=False)
            
            # Find all parts and report
            parts = sorted(glob.glob(f"{base_output}.part_*"))
            total_size = sum(os.path.getsize(part) for part in parts)
            
            print(f"📁 Output: {base_output}.part_**")
            print(f"✅ Backup Complete. {len(parts)} parts, Total: {total_size / (1024**3):.2f} GB")
            
        else:
            # Single file backup
            tar_cmd_parts = ["tar", "-cf", "-"]
            
            if exclude_file:
                tar_cmd_parts.extend(["--exclude-from", exclude_file])
            
            tar_cmd_parts.extend(["-C", os.path.dirname(os.path.abspath(source)), source_name])
            
            # Compression pipeline
            comp_cmd = get_compression_command(compressor)
            pipeline = f"{' '.join(tar_cmd_parts)} | {comp_cmd} > \"{base_output}\""
            
            print(f"📦 Creating single archive...")
            run_command(pipeline, capture_output=False)
            
            file_size = os.path.getsize(base_output)
            print(f"📁 Output: {base_output}")
            print(f"✅ Backup Complete. Single file: {file_size / (1024**3):.2f} GB")
        
        elapsed = time.time() - start_time
        print(f"⏱️  Completed in {elapsed:.1f} seconds")
        
    finally:
        # Clean up exclusion file
        if exclude_file and os.path.exists(exclude_file):
            os.unlink(exclude_file)

def fast_extract(args):
    """Fast extraction using native tar command"""
    source_pattern = args.source
    dest = args.dest
    
    # Handle different input patterns
    if "*" not in source_pattern:
        if "part_" in source_pattern:
            base = source_pattern.split("part_")[0]
            source_pattern = base + "part_*"
        elif source_pattern.endswith((".tar.gz", ".tar.bz2")):
            potential_part = source_pattern + ".part_000"
            if os.path.exists(potential_part):
                source_pattern = source_pattern + ".part_*"
    
    print(f"🔍 Looking for files matching: {source_pattern}")
    
    os.makedirs(dest, exist_ok=True)
    
    files = sorted(glob.glob(source_pattern))
    if not files:
        print(f"❌ No files found matching pattern: {source_pattern}")
        return
    
    print(f"🧩 Extracting from: {source_pattern}")
    
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
            
            # Direct extraction with native tar - keep directory structure
            extract_cmd = f"{decomp_cmd} \"{files[0]}\" | tar -xf - -C \"{dest}\""
            print(f"📦 Extracting...")
            
            run_command(extract_cmd, capture_output=False)
            
        else:
            # Multi-part extraction
            print(f"📦 Multi-part archive detected: {len(files)} parts")
            total_archive_size = sum(os.path.getsize(f) for f in files)
            print(f"📦 Total archive size: {total_archive_size / (1024**3):.2f} GB")
            
            # Concatenate and extract - keep directory structure
            cat_cmd = " ".join([f'cat "{f}"' for f in files])
            extract_cmd = f"{cat_cmd} | {decomp_cmd} | tar -xf - -C \"{dest}\""
            print(f"📦 Extracting multi-part archive...")
            
            run_command(extract_cmd, capture_output=False)
        
        # Calculate extracted size
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
    extract_parser.add_argument('--fast', action='store_true', help='Use fast native extraction (always on in this version)')
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