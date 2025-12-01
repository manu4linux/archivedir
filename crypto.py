#!/usr/bin/env python3
"""
Encryption/Decryption module for archivedir
Provides simple AES-256-CBC encryption using OpenSSL
"""

import os
import sys
import subprocess
import secrets
import hashlib
import getpass


def check_openssl():
    """Check if OpenSSL is available"""
    try:
        result = subprocess.run(['which', 'openssl'], capture_output=True, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def generate_salt():
    """Generate a random 16-byte salt and return as hex string"""
    return secrets.token_bytes(16).hex()


def derive_key(password, salt_hex, iterations=100000):
    """
    Derive encryption key from password using PBKDF2-SHA256
    
    Args:
        password (str): User password
        salt_hex (str): 32-character hex string (16 bytes)
        iterations (int): Number of PBKDF2 iterations
    
    Returns:
        bytes: 32-byte encryption key
    """
    salt = bytes.fromhex(salt_hex)
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt, iterations)


def encrypt_file(input_file, output_file, password, salt=None, iterations=100000):
    """
    Encrypt a file using AES-256-CBC with OpenSSL
    
    Args:
        input_file (str): Path to input file
        output_file (str): Path to output encrypted file
        password (str): Encryption password
        salt (str): Hex salt (32 chars). If None, generates random
        iterations (int): PBKDF2 iterations
    
    Returns:
        str: Hex salt used
    """
    if not check_openssl():
        raise RuntimeError("OpenSSL not found. Please install OpenSSL.")
    
    # Generate or parse salt
    if salt is None:
        salt_bytes = generate_salt()
        salt = salt_bytes.hex()
    else:
        # Validate hex salt
        if len(salt) != 32:
            raise ValueError("Salt must be 32 hex characters (16 bytes)")
        salt_bytes = bytes.fromhex(salt)
    
    # Build OpenSSL command
    cmd = [
        'openssl', 'enc', '-aes-256-cbc', '-pbkdf2',
        '-iter', str(iterations),
        '-salt', '-S', salt,
        '-in', input_file,
        '-out', output_file,
        '-pass', f'pass:{password}'
    ]
    
    # Execute encryption
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"Encryption failed: {result.stderr}")
    
    return salt


def decrypt_file(input_file, output_file, password, salt, iterations=100000):
    """
    Decrypt a file using AES-256-CBC with OpenSSL
    
    Args:
        input_file (str): Path to encrypted input file
        output_file (str): Path to output decrypted file
        password (str): Decryption password
        salt (str): Hex salt (32 chars)
        iterations (int): PBKDF2 iterations
    
    Returns:
        bool: True if successful
    """
    if not check_openssl():
        raise RuntimeError("OpenSSL not found. Please install OpenSSL.")
    
    # Validate hex salt
    if len(salt) != 32:
        raise ValueError("Salt must be 32 hex characters (16 bytes)")
    
    # Build OpenSSL command
    cmd = [
        'openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2',
        '-iter', str(iterations),
        '-salt', '-S', salt,
        '-in', input_file,
        '-out', output_file,
        '-pass', f'pass:{password}'
    ]
    
    # Execute decryption
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"Decryption failed: {result.stderr}")
    
    return True


def encrypt_pipeline_cmd(password, salt, iterations=100000):
    """
    Generate OpenSSL encryption command for pipeline use
    
    Args:
        password (str): Encryption password
        salt (str): Hex salt (32 chars)
        iterations (int): PBKDF2 iterations
    
    Returns:
        str: OpenSSL command string for piping
    """
    return f"openssl enc -aes-256-cbc -pbkdf2 -iter {iterations} -salt -S {salt} -pass pass:{password}"


def decrypt_pipeline_cmd(password, salt, iterations=100000):
    """
    Generate OpenSSL decryption command for pipeline use
    
    Args:
        password (str): Decryption password
        salt (str): Hex salt (32 chars)
        iterations (int): PBKDF2 iterations
    
    Returns:
        str: OpenSSL command string for piping
    """
    return f"openssl enc -d -aes-256-cbc -pbkdf2 -iter {iterations} -salt -S {salt} -pass pass:{password}"


def save_metadata(output_path, salt, iterations):
    """
    Save encryption metadata to .enc file
    
    Args:
        output_path (str): Base path for the encrypted archive
        salt (str): Hex salt
        iterations (int): PBKDF2 iterations
    
    Returns:
        str: Path to metadata file
    """
    metadata_file = f"{output_path}.enc"
    
    with open(metadata_file, 'w') as f:
        f.write(f"salt={salt}\n")
        f.write(f"iterations={iterations}\n")
        f.write(f"algorithm=AES-256-CBC\n")
        f.write(f"kdf=PBKDF2-SHA256\n")
    
    return metadata_file


def load_metadata(archive_path):
    """
    Load encryption metadata from .enc file
    
    Args:
        archive_path (str): Path to encrypted archive or pattern
    
    Returns:
        dict: {'salt': str, 'iterations': int} or None if not found
    """
    # Try to find .enc file
    base_path = archive_path.replace('.part_*', '').replace('.tar.gz.enc', '').replace('.tar.bz2.enc', '').replace('.enc', '')
    metadata_file = f"{base_path}.enc"
    
    if not os.path.exists(metadata_file):
        # Try finding by pattern
        import glob
        pattern = f"{os.path.dirname(base_path)}/*.enc"
        enc_files = glob.glob(pattern)
        if enc_files:
            metadata_file = enc_files[0]
        else:
            return None
    
    salt = None
    iterations = 100000
    
    try:
        with open(metadata_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('salt='):
                    salt = line.split('=', 1)[1]
                elif line.startswith('iterations='):
                    iterations = int(line.split('=', 1)[1])
    except Exception as e:
        print(f"⚠️  Warning: Could not read metadata file: {e}")
        return None
    
    return {'salt': salt, 'iterations': iterations}


def get_password(prompt="🔐 Enter password: ", confirm=False):
    """
    Prompt user for password
    
    Args:
        prompt (str): Password prompt text
        confirm (bool): If True, ask for confirmation
    
    Returns:
        str: Password
    """
    password = getpass.getpass(prompt)
    
    if confirm:
        password_confirm = getpass.getpass("🔐 Confirm password: ")
        if password != password_confirm:
            raise ValueError("Passwords do not match!")
    
    return password


# Example usage
if __name__ == '__main__':
    print("🔐 Crypto Module Test\n")
    
    # Check OpenSSL
    if check_openssl():
        print("✅ OpenSSL is available")
    else:
        print("❌ OpenSSL not found")
        sys.exit(1)
    
    # Test encryption/decryption
    test_file = "/tmp/test_crypto.txt"
    encrypted_file = "/tmp/test_crypto.txt.enc"
    decrypted_file = "/tmp/test_crypto_decrypted.txt"
    
    # Create test file
    with open(test_file, 'w') as f:
        f.write("Hello, this is a test of encryption!\n")
    
    print(f"\n📝 Created test file: {test_file}")
    
    # Encrypt
    password = "TestPassword123"
    salt = encrypt_file(test_file, encrypted_file, password)
    print(f"🔐 Encrypted to: {encrypted_file}")
    print(f"   Salt: {salt}")
    
    # Save metadata
    metadata_file = save_metadata(test_file, salt, 100000)
    print(f"💾 Saved metadata: {metadata_file}")
    
    # Load metadata
    loaded_salt, loaded_iterations = load_metadata(test_file)
    print(f"📖 Loaded metadata: salt={loaded_salt}, iterations={loaded_iterations}")
    
    # Decrypt
    decrypt_file(encrypted_file, decrypted_file, password, salt)
    print(f"🔓 Decrypted to: {decrypted_file}")
    
    # Verify
    with open(decrypted_file, 'r') as f:
        content = f.read()
        if content == "Hello, this is a test of encryption!\n":
            print("✅ Encryption/Decryption test PASSED!")
        else:
            print("❌ Encryption/Decryption test FAILED!")
    
    # Cleanup
    os.remove(test_file)
    os.remove(encrypted_file)
    os.remove(decrypted_file)
    os.remove(metadata_file)
    print("\n🧹 Cleaned up test files")
