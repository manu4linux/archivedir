import os
import sys
import tarfile
import zlib
import argparse
import glob
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from fnmatch import fnmatch

# ================= OPTIONAL IMPORTS =================
try:
    import psutil
    import boto3
    from tqdm import tqdm
except ImportError:
    psutil = None
    boto3 = None
    tqdm = None

# Try to import config.py
try:
    import config
except ImportError:
    config = None

# ================= HELPER CLASSES =================

class SplitFileWriter:
    """
    A file-like object that splits data across multiple physical files
    once a size limit is reached.
    """
    def __init__(self, output_prefix, split_size_bytes, s3_bucket=None, s3_prefix=None):
        self.output_prefix = output_prefix
        self.split_size = split_size_bytes
        self.part_num = 0
        self.current_file = None
        self.current_filename = None
        self.bytes_written_current = 0
        self.total_bytes_written = 0
        
        # S3 Config
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.files_ready_for_upload = []

    def _open_next_file(self):
        if self.current_file:
            self.current_file.close()
            if self.s3_bucket:
                self.files_ready_for_upload.append(self.current_filename)

        part_suffix = f"{self.part_num:03d}"  # e.g., .000, .001
        self.current_filename = f"{self.output_prefix}{part_suffix}"
        self.current_file = open(self.current_filename, 'wb')
        self.part_num += 1
        self.bytes_written_current = 0

    def write(self, data):
        if not self.current_file:
            self._open_next_file()

        data_len = len(data)
        
        if self.bytes_written_current + data_len > self.split_size:
            remaining_space = self.split_size - self.bytes_written_current
            self.current_file.write(data[:remaining_space])
            self._open_next_file()
            self.write(data[remaining_space:]) 
        else:
            self.current_file.write(data)
            self.bytes_written_current += data_len
            self.total_bytes_written += data_len

    def close(self):
        if self.current_file:
            self.current_file.close()
            if self.s3_bucket:
                self.files_ready_for_upload.append(self.current_filename)

class ParallelGzipWriter:
    """
    Writes a GZIP stream using multiple threads. 
    """
    def __init__(self, sink_file_obj, level=1, threads=None):
        self.sink = sink_file_obj
        self.level = level
        self.chunk_size = 2 * 1024 * 1024  # 2MB chunks for optimal cache
        self.buffer = BytesIO()
        self.workers = threads or os.cpu_count()
        self.executor = ThreadPoolExecutor(max_workers=self.workers)
        self.futures = []

    def _compress_chunk(self, data):
        # Concatenated Gzip members are valid GZIP files.
        compressor = zlib.compressobj(self.level, zlib.DEFLATED, 31)
        return compressor.compress(data) + compressor.flush()

    def write(self, data):
        self.buffer.write(data)
        if self.buffer.tell() >= self.chunk_size:
            self._flush_buffer()

    def _flush_buffer(self):
        data = self.buffer.getvalue()
        self.buffer = BytesIO()
        if not data: return
        
        future = self.executor.submit(self._compress_chunk, data)
        self.futures.append(future)
        
        if len(self.futures) > self.workers * 2:
            self._drain_futures()

    def _drain_futures(self):
        while self.futures:
            future = self.futures.pop(0)
            self.sink.write(future.result())

    def close(self):
        self._flush_buffer()
        self._drain_futures()
        self.executor.shutdown()
        self.sink.close()

# ================= CORE LOGIC =================

def get_fs_limit(path):
    """Check for FAT32 and return safe size limit."""
    if not psutil: return None
    try:
        parts = psutil.disk_partitions()
        abs_path = os.path.abspath(path)
        best_match = ""
        fs_type = ""
        for p in parts:
            if abs_path.startswith(p.mountpoint) and len(p.mountpoint) > len(best_match):
                best_match = p.mountpoint
                fs_type = p.fstype.lower()
        
        if 'fat' in fs_type or 'msdos' in fs_type:
            return 3.9 * 1024 * 1024 * 1024 
    except Exception:
        pass
    return None

def run_backup(args):
    # --- Input Validation (Post-Merge) ---
    if not args.source:
        print("❌ Error: No Source specified (neither in CLI args nor config.py).")
        sys.exit(1)
    if not args.dest:
        print("❌ Error: No Destination specified (neither in CLI args nor config.py).")
        sys.exit(1)

    # Resolve paths
    sources = [os.path.abspath(s) for s in args.source]
    dest = args.dest
    
    # S3 Setup
    is_s3 = dest.startswith("s3://")
    s3_bucket, s3_prefix = None, None
    local_dest = dest
    
    if is_s3:
        if not boto3:
            print("❌ Error: 'boto3' not installed. Cannot use S3.")
            sys.exit(1)
        path_parts = dest.replace("s3://", "").split("/", 1)
        s3_bucket = path_parts[0]
        s3_prefix = path_parts[1] if len(path_parts) > 1 else ""
        local_dest = os.getcwd() 
        print(f"ℹ️  S3 detected. Staging locally at {local_dest} then uploading.")
    else:
        if not os.path.exists(local_dest):
            os.makedirs(local_dest, exist_ok=True)

    # Size Config
    split_bytes = int(args.size * 1024 * 1024 * 1024)
    if not is_s3:
        fs_limit = get_fs_limit(local_dest)
        if fs_limit and split_bytes > fs_limit:
            print(f"⚠️  FAT32 Filesystem detected. Reducing split size to 3.9 GB.")
            split_bytes = fs_limit

    # Naming
    base_name = os.path.basename(sources[0]) if len(sources) == 1 else "multi_backup"
    output_prefix = os.path.join(local_dest, f"{base_name}.tar.gz.part_")

    # Pipeline Init
    split_writer = SplitFileWriter(output_prefix, split_bytes, s3_bucket, s3_prefix)
    compressor = ParallelGzipWriter(split_writer, level=args.compress_level)
    
    print(f"🚀 Starting Backup")
    print(f"📂 Sources: {', '.join(sources)}")
    print(f"💾 Destination: {local_dest}")
    if args.exclude:
        print(f"🚫 Excluding: {', '.join(args.exclude)}")
    print(f"✂️  Split Size: {args.size} GB")

    # Progress Bar
    total_size = 0
    if tqdm:
        print("⏳ Calculating total size...")
        for src in sources:
            if os.path.isfile(src):
                total_size += os.path.getsize(src)
            else:
                for root, dirs, files in os.walk(src):
                    for f in files:
                        fp = os.path.join(root, f)
                        if not os.path.islink(fp):
                            total_size += os.path.getsize(fp)
        pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc="Archiving")

    # Tar Stream
    tar = tarfile.open(fileobj=compressor, mode='w|', format=tarfile.PAX_FORMAT)

    try:
        excludes = args.exclude or []
        
        def _filter(tarinfo):
            for ex in excludes:
                if fnmatch(tarinfo.name, ex) or ex in tarinfo.name:
                    return None
            if tqdm: pbar.update(tarinfo.size)
            return tarinfo

        for src in sources:
            arcname = os.path.basename(src) 
            tar.add(src, arcname=arcname, filter=_filter)

    except KeyboardInterrupt:
        print("\n❌ Aborted.")
        sys.exit(1)
    finally:
        tar.close()
        compressor.close()
        split_writer.close()
        if tqdm: pbar.close()

    # S3 Upload
    if is_s3 and split_writer.files_ready_for_upload:
        print("\n☁️  Uploading generated parts to S3...")
        s3 = boto3.client('s3')
        for fname in split_writer.files_ready_for_upload:
            key = os.path.join(s3_prefix, os.path.basename(fname))
            print(f"   ⬆️  {os.path.basename(fname)}")
            s3.upload_file(fname, s3_bucket, key)

    print("\n✅ Backup Complete.")

class MultiPartFileReader:
    def __init__(self, file_pattern):
        self.files = sorted(glob.glob(file_pattern))
        if not self.files:
            raise FileNotFoundError("No split files found")
        self.current_idx = 0
        self.current_file = open(self.files[0], 'rb')
    
    def read(self, size=-1):
        data = self.current_file.read(size)
        while not data and self.current_idx < len(self.files) - 1:
            self.current_file.close()
            self.current_idx += 1
            self.current_file = open(self.files[self.current_idx], 'rb')
            data = self.current_file.read(size)
        return data

    def close(self):
        if self.current_file:
            self.current_file.close()

def run_extract(args):
    # --- Input Validation (Post-Merge) ---
    if not args.source:
        print("❌ Error: No Source specified (neither in CLI args nor config.py).")
        sys.exit(1)
    if not args.dest:
        print("❌ Error: No Destination specified (neither in CLI args nor config.py).")
        sys.exit(1)

    source_pattern = args.source
    if "*" not in source_pattern and "part_" in source_pattern:
        base = source_pattern.split("part_")[0]
        source_pattern = base + "part_*"
    
    dest = args.dest
    if not os.path.exists(dest):
        os.makedirs(dest, exist_ok=True)

    print(f"🧩 Extracting from: {source_pattern}")
    try:
        reader = MultiPartFileReader(source_pattern)
        tar = tarfile.open(fileobj=reader, mode='r|gz')
        tar.extractall(path=dest)
        tar.close()
        reader.close()
        print("✅ Extraction Complete.")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def get_config_val(var_name, default=None):
    """Safely get value from config module."""
    if config:
        return getattr(config, var_name, default)
    return default

def main():
    parser = argparse.ArgumentParser(description="Multi-Source Pure Python Archive Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Backup Args (Defaults set to None to allow merge) ---
    bp = subparsers.add_parser('backup')
    bp.add_argument('--source', '-s', nargs='+', help="List of files/folders to backup")
    bp.add_argument('--dest', '-d', help="Destination folder or s3://bucket/key")
    bp.add_argument('--size', '-sz', type=float, help="Split size in GB")
    bp.add_argument('--compress-level', '-l', type=int, help="1 (fast) to 9 (slow)")
    bp.add_argument('--exclude', '-e', action='append', help="Pattern to exclude")

    # --- Extract Args ---
    ep = subparsers.add_parser('extract')
    ep.add_argument('--source', '-s', help="Path pattern (e.g. /mnt/usb/data.part_*)")
    ep.add_argument('--dest', '-d', help="Extraction destination")

    args = parser.parse_args()

    # ================= MERGE LOGIC =================
    # Priority: 1. CLI Args -> 2. Config File -> 3. Defaults
    
    if args.command == 'backup':
        # 1. Sources
        if args.source is None:
            args.source = get_config_val('BACKUP_SOURCES')
        
        # 2. Destination
        if args.dest is None:
            args.dest = get_config_val('DESTINATION')
        
        # 3. Size (Default 3.5)
        if args.size is None:
            args.size = get_config_val('SPLIT_SIZE_GB', 3.5)
        
        # 4. Compression Level (Default 1)
        if args.compress_level is None:
            args.compress_level = get_config_val('COMPRESSION_LEVEL', 1)
            
        # 5. Excludes (Default [])
        # If CLI gave excludes, we use those. If not, check config. 
        if args.exclude is None:
            args.exclude = get_config_val('EXCLUDES', [])
        
        run_backup(args)

    elif args.command == 'extract':
        if args.source is None:
            args.source = get_config_val('EXTRACT_SOURCE')
        
        if args.dest is None:
            args.dest = get_config_val('EXTRACT_DEST')

        run_extract(args)

if __name__ == "__main__":
    main()