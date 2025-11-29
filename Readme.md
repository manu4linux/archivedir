# **ArchiveDir Tool**

**ArchiveDir** is a high-performance, pure Python utility designed to create and extract split, compressed archives. It replaces complex shell pipelines (like tar | pigz | split) with a single, cross-platform script that runs on Windows, Linux, and macOS without external binary dependencies.

## **Key Features**

* **Parallel Compression:** Uses multi-threading to utilize all CPU cores, achieving speeds comparable to pigz.  
* **Smart Splitting:** Splits archives into chunks (default 3.5GB) to fit on FAT32 drives or for easier cloud upload.  
* **FAT32 Auto-Detection:** Automatically detects if the destination drive is FAT32 and forces a safe split limit (3.9GB) to prevent errors.  
* **S3 Support:** Can stage and upload split parts directly to an AWS S3 bucket.  
* **Flexible Configuration:** Configure via Command Line Arguments (CLI) or a config.py file.

## **Prerequisites**

* **Python 3.8+**  
* **Dependencies:** The script runs with standard libraries, but the following are highly recommended for full functionality:

pip install tqdm psutil boto3

* tqdm: Shows progress bars (highly recommended for large files).  
* psutil: Required for FAT32 filesystem auto-detection.  
* boto3: Required only if using S3 destinations.

## **Configuration (config.py)**

You can define default values in config.py to avoid typing long commands. The tool follows this priority order:

1. **CLI Arguments** (Highest Priority)  
2. **Config File Values**  
3. **Hardcoded Defaults** (Lowest Priority)

**Example config.py setup:**

BACKUP\_SOURCES \= \["/home/user/documents", "/var/www"\]  
DESTINATION \= "/mnt/usb\_backup"  
SPLIT\_SIZE\_GB \= 4.0  
EXCLUDES \= \["\*.log", "tmp", ".git"\]

## **Usage Examples**

### **1\. Backup Mode**

Basic Backup to USB  
Backs up a folder to a USB drive, splitting files every 3.5GB (default).  
python3 archivedir.py backup \--source /home/user/data \--dest /mnt/usb\_drive

Multiple Sources with Exclusions  
Backs up two different folders, excludes .log files and node\_modules, and sets a custom split size of 2GB.  
python3 archivedir.py backup \\  
  \--source /home/user/docs /home/user/photos \\  
  \--dest /mnt/backup\_drive \\  
  \--exclude "\*.log" \\  
  \--exclude "node\_modules" \\  
  \--size 2

Backup Using Config File  
If you have set BACKUP\_SOURCES and DESTINATION in config.py, you can simply run:  
python3 archivedir.py backup

*You can still override specific settings:*

\# Uses sources from config, but overrides destination  
python3 archivedir.py backup \--dest /tmp/alt\_backup

Backup to AWS S3  
Stages files locally (in the current directory) and uploads them to the specified bucket.  
python3 archivedir.py backup \--source /var/data \--dest s3://my-backup-bucket/daily/

*(Requires boto3 installed and AWS credentials configured via \~/.aws/credentials or environment variables)*

### **2\. Extraction Mode**

Extracting an Archive  
Point the tool to the split files. You can use a wildcard or just the prefix.  
python3 archivedir.py extract \--source "/mnt/usb/data.tar.gz.part\_\*" \--dest /home/user/restore\_location

Extracting using Config File  
If EXTRACT\_SOURCE and EXTRACT\_DEST are set in config.py:  
python3 archivedir.py extract

## **Building a Standalone Executable**

If you need to run this on a machine that doesn't have Python installed, you can compile it into a single executable file (EXE on Windows, Binary on Linux/Mac).

1. **Install PyInstaller:**  
   pip install pyinstaller

2. **Build the binary:**  
   pyinstaller \--onefile archivedir.py

3. Run:  
   The executable will be located in the dist/ folder.  
   * **Linux/Mac:** ./dist/archivedir backup \-s /data \-d /usb  
   * **Windows:** dist\\archivedir.exe backup \-s C:\\Data \-d D:\\Backup