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
    
    def get_part_count(self):
        """Return the number of parts created."""
        return self.part_num
    
    def is_single_file(self):
        """Return True if only one part was created (no splitting occurred)."""
        return self.part_num == 1
    
    def get_final_filename(self):
        """Get the final filename, handling single file case."""
        if self.is_single_file() and self.current_filename:
            # For single files, we might want to rename without .part_000 suffix
            base_name = self.output_prefix.rstrip('_')
            if base_name.endswith('.part'):
                base_name = base_name[:-5]  # Remove .part suffix
            return base_name
        return self.current_filename
    
    def get_part_count(self):
        """Return the number of parts created."""
        return self.part_num
    
    def is_single_file(self):
        """Return True if only one part was created (no splitting occurred)."""
        return self.part_num == 1
    
    def get_final_filename(self):
        """Get the final filename, handling single file case."""
        if self.is_single_file() and self.current_filename:
            # For single files, we might want to rename without .part_000 suffix
            base_name = self.output_prefix.rstrip('_')
            if base_name.endswith('.part'):
                base_name = base_name[:-5]  # Remove .part suffix
            return base_name
        return self.current_filename

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
        
        # Add default exclusions for problematic files that cause corruption
        default_excludes = []
        if not getattr(args, 'include_problematic', False):
            default_excludes = [
                "*.dill",  # Flutter/Dart compiled binary files (often corrupted)
                "*.so.debug",  # Debug symbols that can be corrupted
                "*.pdb",  # Program database files
                "*.tmp",  # Temporary files
                "*/.dart_tool/flutter_build/*/app.dill",  # Specific corrupted file pattern
                "*/.dart_tool/flutter_build/*/kernel_snapshot.dill",
                "*/.dart_tool/build/*/app.dill",
                "*/node_modules/.cache/*",  # Node.js cache files
                "*/__pycache__/*",  # Python bytecode cache
                "*.pyc",  # Python compiled files
                "*.core",  # Core dump files
                "*/build/intermediates/*",  # Android build intermediates
                "*/.gradle/*/executionHistory/*",  # Gradle execution history
                "*/.gradle/*/fileHashes/*",  # Gradle file hashes
                "*/.gradle/buildOutputCleanup/*",  # Gradle cleanup cache
                "*/.gradle/vcs-*/*",  # Gradle VCS cache
                "*.lock",  # Lock files that can be corrupted during backup
                "*/flutter_build/*",  # Entire flutter build directory
                "*/.dart_tool/chrome-device/*",  # Chrome device cache
            ]
        
        # Combine user excludes with defaults
        all_excludes = excludes + default_excludes
        
        if default_excludes:
            print(f"🛡️  Auto-excluding {len(default_excludes)} problematic file patterns (use --include-problematic to override)")
        
        def _filter(tarinfo):
            # Check both user-defined and default excludes
            for ex in all_excludes:
                if fnmatch(tarinfo.name, ex) or ex in tarinfo.name:
                    if getattr(args, 'verbose', False):
                        if ex in default_excludes:
                            print(f"🚫 Auto-excluding problematic file: {tarinfo.name}")
                        else:
                            print(f"🚫 User-excluding file: {tarinfo.name}")
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

    # Handle single file renaming for cleaner output
    final_output_path = None
    if split_writer.is_single_file():
        old_filename = split_writer.current_filename
        new_filename = split_writer.get_final_filename()
        if old_filename != new_filename and os.path.exists(old_filename):
            try:
                os.rename(old_filename, new_filename)
                final_output_path = new_filename
                print(f"📝 Renamed single file: {os.path.basename(new_filename)}")
            except OSError as e:
                print(f"⚠️  Could not rename {old_filename}: {e}")
                final_output_path = old_filename
        else:
            final_output_path = split_writer.current_filename or old_filename
    else:
        # For multi-part, show the pattern
        final_output_path = split_writer.current_filename
        if final_output_path:
            # Convert /path/file.tar.gz.part_002 to /path/file.tar.gz.part_*
            if '.part_' in final_output_path:
                base_path = final_output_path.split('.part_')[0]
                final_output_path = base_path + ".part_*"
    
    part_count = split_writer.get_part_count()
    total_gb = split_writer.total_bytes_written / (1024*1024*1024)
    
    # Show full output path with part indicator
    parts_indicator = "*" if part_count > 1 else ""
    
    if final_output_path:
        print(f"\n📁 Output: {final_output_path}{parts_indicator}")
    
    if part_count == 1:
        print(f"✅ Backup Complete. Single file: {total_gb:.2f} GB")
    else:
        print(f"✅ Backup Complete. {part_count} parts, Total: {total_gb:.2f} GB")

class MultiPartFileReader:
    def __init__(self, file_pattern, buffer_size=None):
        self.files = sorted(glob.glob(file_pattern))
        if not self.files:
            raise FileNotFoundError("No split files found")
        self.current_idx = 0
        
        # Auto-adjust buffer size based on total archive size
        if buffer_size is None:
            total_size = sum(os.path.getsize(f) for f in self.files)
            if total_size > 50 * 1024 * 1024 * 1024:  # > 50GB
                self.buffer_size = 512 * 1024  # 512KB buffer for huge files
                print(f"🔧 Large archive detected ({total_size / (1024**3):.1f}GB), using small buffers to conserve memory")
            elif total_size > 5 * 1024 * 1024 * 1024:  # > 5GB  
                self.buffer_size = 1 * 1024 * 1024  # 1MB buffer
            else:
                self.buffer_size = 2 * 1024 * 1024  # 2MB buffer for smaller files
        else:
            self.buffer_size = buffer_size
            
        self.current_file = open(self.files[0], 'rb', buffering=self.buffer_size)
    
    def read(self, size=-1):
        if size == -1:
            # Read all remaining data
            chunks = []
            while self.current_idx < len(self.files):
                chunk = self.current_file.read()
                if chunk:
                    chunks.append(chunk)
                if self.current_idx < len(self.files) - 1:
                    self.current_file.close()
                    self.current_idx += 1
                    self.current_file = open(self.files[self.current_idx], 'rb', buffering=self.buffer_size)
                else:
                    break
            return b''.join(chunks)
        else:
            # Read specific size
            data = self.current_file.read(size)
            while len(data) < size and self.current_idx < len(self.files) - 1:
                self.current_file.close()
                self.current_idx += 1
                self.current_file = open(self.files[self.current_idx], 'rb', buffering=self.buffer_size)
                remaining = size - len(data)
                additional = self.current_file.read(remaining)
                data += additional
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
    
    # Handle different input patterns
    if "*" not in source_pattern:
        if "part_" in source_pattern:
            # Handle patterns like /path/file.part_000 -> /path/file.part_*
            base = source_pattern.split("part_")[0]
            source_pattern = base + "part_*"
        elif source_pattern.endswith(".tar.gz"):
            # Check if it's actually a split file with .part_000 extension
            potential_part = source_pattern + ".part_000"
            if os.path.exists(potential_part):
                source_pattern = source_pattern + ".part_*"
    
    print(f"🔍 Looking for files matching: {source_pattern}")
    
    dest = args.dest
    if not os.path.exists(dest):
        os.makedirs(dest, exist_ok=True)

    print(f"🧩 Extracting from: {source_pattern}")
    
    files = sorted(glob.glob(source_pattern))
    if not files:
        print(f"❌ No files found matching pattern: {source_pattern}")
        return
        
    # Fast path for single files - skip MultiPartFileReader overhead
    if len(files) == 1:
        print(f"📁 Single file detected: {os.path.basename(files[0])}")
        try:
            # Use streaming mode even for single files to avoid memory issues with huge archives
            with open(files[0], 'rb') as f:
                print("📦 Extracting...")
                tar = tarfile.open(fileobj=f, mode='r|gz')  # Streaming mode for memory efficiency
                files_extracted = 0
                
                try:
                    # Add per-file timeout to prevent hanging on corrupted files
                    import signal
                    
                    def file_timeout_handler(signum, frame):
                        raise TimeoutError("File extraction timed out - likely corrupted")
                    
                    if tqdm:
                        # Use streaming extraction with live progress counter
                        with tqdm(desc="Extracting", unit=" files") as pbar:
                            for member in tar:
                                try:
                                    print(f"🔄 Extracting: {member.name[:50]}{'...' if len(member.name) > 50 else ''}")
                                    
                                    # Set 30-second timeout per file
                                    signal.signal(signal.SIGALRM, file_timeout_handler)
                                    signal.alarm(30)
                                    
                                    tar.extract(member, path=dest)
                                    
                                    # Cancel timeout if extraction succeeds
                                    signal.alarm(0)
                                    
                                    files_extracted += 1
                                    pbar.update(1)
                                    if files_extracted % 100 == 0:
                                        print(f"📊 Extracted {files_extracted} files so far...")
                                        
                                except (Exception, TimeoutError) as e:
                                    signal.alarm(0)  # Cancel timeout
                                    should_continue = (hasattr(args, 'continue_on_error') and args.continue_on_error) and not (hasattr(args, 'stop_on_error') and args.stop_on_error)
                                    if should_continue:
                                        print(f"⚠️  Skipped corrupted/timeout file {member.name}: {e}")
                                        continue
                                    else:
                                        print(f"❌ Stopping on error: {e}")
                                        raise
                    else:
                        # Stream extract without loading all members into memory
                        # Handle corruption during tar iteration
                        archive_size = os.path.getsize(files[0]) if files else 0
                        print(f"📦 Archive size: {archive_size / (1024**3):.2f} GB")
                        
                        try:
                            tar_iterator = iter(tar)
                            while True:
                                try:
                                    member = next(tar_iterator)
                                    try:
                                        print(f"🔄 Extracting: {member.name[:50]}{'...' if len(member.name) > 50 else ''}")
                                        
                                        # Set 30-second timeout per file
                                        signal.signal(signal.SIGALRM, file_timeout_handler)
                                        signal.alarm(30)
                                        
                                        tar.extract(member, path=dest)
                                        
                                        # Cancel timeout if extraction succeeds
                                        signal.alarm(0)
                                        
                                        files_extracted += 1
                                        if files_extracted % 100 == 0:
                                            print(f"📊 Extracted {files_extracted} files so far...")
                                            
                                    except (Exception, TimeoutError) as e:
                                        signal.alarm(0)  # Cancel timeout
                                        should_continue = (hasattr(args, 'continue_on_error') and args.continue_on_error) and not (hasattr(args, 'stop_on_error') and args.stop_on_error)
                                        if should_continue:
                                            print(f"⚠️  Skipped corrupted/timeout file {member.name}: {e}")
                                            continue
                                        else:
                                            print(f"❌ Stopping on error: {e}")
                                            raise
                                            
                                except StopIteration:
                                    # Normal end of archive
                                    break
                                except Exception as iter_e:
                                    # Corruption during tar member iteration
                                    should_continue = (hasattr(args, 'continue_on_error') and args.continue_on_error) and not (hasattr(args, 'stop_on_error') and args.stop_on_error)
                                    if should_continue:
                                        print(f"⚠️  Archive corruption detected, stopping extraction: {iter_e}")
                                        print(f"📊 Managed to extract {files_extracted} files before corruption")
                                        break
                                    else:
                                        print(f"❌ Archive corruption: {iter_e}")
                                        raise
                        except Exception as outer_e:
                            print(f"⚠️  Archive read error: {outer_e}")
                except (tarfile.ReadError, EOFError, OSError) as e:
                    should_continue = (hasattr(args, 'continue_on_error') and args.continue_on_error) and not (hasattr(args, 'stop_on_error') and args.stop_on_error)
                    if should_continue:
                        print(f"⚠️  Archive error after {files_extracted} files: {e}")
                        print(f"✅ Partial extraction completed: {files_extracted} files extracted")
                        return
                    else:
                        raise
                tar.close()
            
            # Report extraction statistics
            extracted_size_bytes = sum(os.path.getsize(os.path.join(root, f)) 
                                      for root, _, files_list in os.walk(dest) 
                                      for f in files_list if os.path.exists(os.path.join(root, f)))
            
            # Auto-scale the size units
            if extracted_size_bytes >= 1024**3:  # GB
                extracted_size = extracted_size_bytes / (1024**3)
                extracted_unit = "GB"
            elif extracted_size_bytes >= 1024**2:  # MB
                extracted_size = extracted_size_bytes / (1024**2)
                extracted_unit = "MB"
            else:  # KB or bytes
                extracted_size = extracted_size_bytes / 1024
                extracted_unit = "KB"
            
            archive_size_gb = archive_size / (1024**3)
            compression_ratio = (archive_size / extracted_size_bytes) if extracted_size_bytes > 0 else 0
            
            print(f"✅ Extraction Complete: {files_extracted} files, {extracted_size:.1f} {extracted_unit} extracted from {archive_size_gb:.2f} GB archive (compression: {compression_ratio:.1f}x)")
            return
        except Exception as e:
            print(f"❌ Single file extraction failed: {e}")
            # Fall through to multi-part method
    
    # Multi-part file extraction
    print(f"📁 Multi-part archive detected: {len(files)} parts")
    
    # Add early corruption detection
    print("🔍 Testing archive header...")
    try:
        # Quick test - try to read just the first member
        test_reader = MultiPartFileReader(source_pattern, buffer_size=64*1024)
        test_tar = tarfile.open(fileobj=test_reader, mode='r|gz')
        try:
            first_member = next(iter(test_tar))
            print(f"✅ Archive header OK, first member: {first_member.name}")
        except StopIteration:
            print("❌ Archive appears to be empty")
            return
        except Exception as e:
            print(f"❌ Archive header corrupted: {e}")
            return
        finally:
            test_tar.close()
            test_reader.close()
    except Exception as e:
        print(f"❌ Cannot open archive: {e}")
        return
    
    try:
        # Use smaller buffers for low-memory mode
        buffer_size = 64*1024 if hasattr(args, 'low_memory') and args.low_memory else None  # 64KB for low-memory
        reader = MultiPartFileReader(source_pattern, buffer_size=buffer_size)
        
        print("🔧 Opening tar stream for extraction...")
        tar = tarfile.open(fileobj=reader, mode='r|gz')  # Use streaming mode for multi-part
        
        print("📦 Starting extraction...")
        
        # Show memory usage tip for large archives
        if hasattr(args, 'low_memory') and not args.low_memory:
            total_size = sum(os.path.getsize(f) for f in files)
            if total_size > 20 * 1024 * 1024 * 1024:  # > 20GB
                print("💡 Tip: For very large archives, use --low-memory to reduce RAM usage")
        
        # Show initial memory usage if psutil available
        if psutil:
            process = psutil.Process()
            initial_mem = process.memory_info().rss / (1024*1024)  # MB
            print(f"🧠 Initial memory usage: {initial_mem:.1f} MB")
        
        # Add timeout and better error handling for corrupted archives
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Extraction timed out - archive may be corrupted")
        
        # Set a reasonable timeout (5 minutes for large archives)
        if not hasattr(args, 'fast') or not args.fast:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(300)  # 5 minute timeout
        
        try:
            files_extracted = 0
            # Add per-file timeout for multi-part extraction too
            def file_timeout_handler(signum, frame):
                raise TimeoutError("File extraction timed out - likely corrupted")
                
            if tqdm:
                # For streaming mode, we can't get member count ahead of time
                # So we'll just show a spinner-style progress
                with tqdm(desc="Extracting", unit=" files") as pbar:
                    for member in tar:
                        try:
                            print(f"🔄 Extracting: {member.name[:50]}{'...' if len(member.name) > 50 else ''}")
                            
                            # Set 30-second timeout per file
                            signal.signal(signal.SIGALRM, file_timeout_handler)
                            signal.alarm(30)
                            
                            tar.extract(member, path=dest)
                            
                            # Cancel timeout if extraction succeeds
                            signal.alarm(0)
                            
                            files_extracted += 1
                            pbar.update(1)
                            if files_extracted % 100 == 0:  # Progress update every 100 files
                                print(f"📊 Extracted {files_extracted} files so far...")
                                
                        except (Exception, TimeoutError) as e:
                            signal.alarm(0)  # Cancel timeout
                            should_continue = (hasattr(args, 'continue_on_error') and args.continue_on_error) and not (hasattr(args, 'stop_on_error') and args.stop_on_error)
                            if should_continue:
                                print(f"⚠️  Skipped corrupted/timeout file {member.name}: {e}")
                                continue
                            else:
                                print(f"❌ Stopping on error: {e}")
                                raise
            else:
                # Manual extraction with progress
                for member in tar:
                    try:
                        print(f"🔄 Extracting: {member.name[:50]}{'...' if len(member.name) > 50 else ''}")
                        
                        # Set 30-second timeout per file
                        signal.signal(signal.SIGALRM, file_timeout_handler)
                        signal.alarm(30)
                        
                        tar.extract(member, path=dest)
                        
                        # Cancel timeout if extraction succeeds
                        signal.alarm(0)
                        
                        files_extracted += 1
                        if files_extracted % 100 == 0:
                            print(f"📊 Extracted {files_extracted} files so far...")
                            
                    except (Exception, TimeoutError) as e:
                        signal.alarm(0)  # Cancel timeout
                        should_continue = (hasattr(args, 'continue_on_error') and args.continue_on_error) and not (hasattr(args, 'stop_on_error') and args.stop_on_error)
                        if should_continue:
                            print(f"⚠️  Skipped corrupted/timeout file {member.name}: {e}")
                            continue
                        else:
                            print(f"❌ Stopping on error: {e}")
                            raise
        
        except (tarfile.ReadError, EOFError, OSError) as e:
            print(f"⚠️  Archive corruption detected after {files_extracted} files: {e}")
            print(f"✅ Partial extraction completed: {files_extracted} files extracted")
            return
        finally:
            if not hasattr(args, 'fast') or not args.fast:
                signal.alarm(0)  # Cancel timeout
            
        tar.close()
        reader.close()
        
        # Show final memory usage if psutil available
        if psutil:
            process = psutil.Process()
            final_mem = process.memory_info().rss / (1024*1024)  # MB
            print(f"🧠 Peak memory usage: {final_mem:.1f} MB")
        
        print(f"✅ Extraction Complete (multi-part): {files_extracted} files extracted")
        
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
    bp.add_argument('--verbose', '-v', action='store_true', help="Show detailed output including auto-excluded files")
    bp.add_argument('--include-problematic', action='store_true', help="Include files that commonly cause corruption (*.dill, etc.)")

    # --- Extract Args ---
    ep = subparsers.add_parser('extract')
    ep.add_argument('--source', '-s', help="Path pattern (e.g. /mnt/usb/data.part_*)")
    ep.add_argument('--dest', '-d', help="Extraction destination")
    ep.add_argument('--fast', '-f', action='store_true', help="Skip integrity checks for faster extraction")
    ep.add_argument('--low-memory', action='store_true', help="Use minimal memory for very large archives")
    ep.add_argument('--continue-on-error', action='store_true', default=True, help="Continue extraction even if some files are corrupted (default: True)")
    ep.add_argument('--stop-on-error', action='store_true', help="Stop extraction on first error (overrides default continue behavior)")

    # --- Test Args ---
    tp = subparsers.add_parser('test', help="Test archive integrity without extracting")
    tp.add_argument('--source', '-s', help="Path pattern (e.g. /mnt/usb/data.part_*)")

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

    elif args.command == 'test':
        if args.source is None:
            args.source = get_config_val('EXTRACT_SOURCE')
        
        run_test_archive(args)

def run_test_archive(args):
    """Test archive integrity without extracting."""
    if not args.source:
        print("❌ Error: No Source specified.")
        sys.exit(1)
    
    source_pattern = args.source
    
    # Handle different input patterns
    if "*" not in source_pattern:
        if "part_" in source_pattern:
            # Handle patterns like /path/file.part_000 -> /path/file.part_*
            base = source_pattern.split("part_")[0]
            source_pattern = base + "part_*"
        elif source_pattern.endswith(".tar.gz"):
            # Check if it's actually a split file with .part_000 extension
            potential_part = source_pattern + ".part_000"
            if os.path.exists(potential_part):
                source_pattern = source_pattern + ".part_*"
    
    print(f"🔍 Testing archive integrity: {source_pattern}")
    try:
        files = sorted(glob.glob(source_pattern))
        if not files:
            print(f"❌ No files found matching pattern: {source_pattern}")
            return
            
        print(f"📁 Found {len(files)} part file(s)")
        for i, f in enumerate(files):
            size = os.path.getsize(f) / (1024*1024)
            print(f"   Part {i}: {f} ({size:.1f} MB)")
        
        reader = MultiPartFileReader(source_pattern)
        tar = tarfile.open(fileobj=reader, mode='r|gz')
        
        member_count = 0
        total_size = 0
        
        if tqdm:
            print("🧪 Testing archive members...")
            # First pass to count members
            test_reader = MultiPartFileReader(source_pattern)
            test_tar = tarfile.open(fileobj=test_reader, mode='r|gz')
            members = []
            for member in test_tar:
                members.append(member)
            test_tar.close()
            test_reader.close()
            
            # Second pass with progress
            reader = MultiPartFileReader(source_pattern)
            tar = tarfile.open(fileobj=reader, mode='r|gz')
            
            for member in tqdm(tar, total=len(members), desc="Testing"):
                member_count += 1
                total_size += member.size
                # Try to read a bit of each file to verify integrity
                if member.isfile():
                    try:
                        f = tar.extractfile(member)
                        if f:
                            f.read(1024)  # Read first 1KB to test
                    except Exception as e:
                        print(f"❌ Error reading member {member.name}: {e}")
                        raise
        else:
            print("🧪 Testing archive members...")
            for member in tar:
                member_count += 1
                total_size += member.size
                if member.isfile():
                    try:
                        f = tar.extractfile(member)
                        if f:
                            f.read(1024)  # Read first 1KB to test
                    except Exception as e:
                        print(f"❌ Error reading member {member.name}: {e}")
                        raise
        
        tar.close()
        reader.close()
        
        print(f"✅ Archive integrity test PASSED")
        print(f"📊 Summary:")
        print(f"   • {member_count} members tested")
        print(f"   • {total_size / (1024*1024*1024):.2f} GB total uncompressed size")
        print(f"   • {len(files)} part file(s)")
        
    except Exception as e:
        print(f"❌ Archive integrity test FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()