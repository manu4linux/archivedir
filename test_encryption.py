#!/usr/bin/env python3
"""
Test script for encryption/decryption functionality
"""

import os
import sys
import tempfile
import shutil

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import crypto
    print("✅ crypto.py module imported successfully")
except ImportError as e:
    print(f"❌ Failed to import crypto.py: {e}")
    sys.exit(1)

def test_openssl():
    """Test OpenSSL availability"""
    print("\n🔍 Testing OpenSSL...")
    if crypto.check_openssl():
        print("   ✅ OpenSSL is available")
        return True
    else:
        print("   ❌ OpenSSL not found")
        return False

def test_salt_generation():
    """Test salt generation"""
    print("\n🔍 Testing salt generation...")
    salt = crypto.generate_salt()
    print(f"   Generated salt: {salt[:16]}...")
    if len(salt) == 32:  # 16 bytes = 32 hex chars
        print(f"   ✅ Salt length correct (32 hex chars)")
        return True
    else:
        print(f"   ❌ Salt length incorrect: {len(salt)}")
        return False

def test_key_derivation():
    """Test key derivation"""
    print("\n🔍 Testing key derivation...")
    password = "TestPassword123!"
    salt_hex = crypto.generate_salt()
    
    try:
        key = crypto.derive_key(password, salt_hex, iterations=10000)
        print(f"   Generated key: {key[:16]}...")
        print(f"   ✅ Key derivation successful")
        return True
    except Exception as e:
        print(f"   ❌ Key derivation failed: {e}")
        return False

def test_pipeline_commands():
    """Test pipeline command generation"""
    print("\n🔍 Testing pipeline commands...")
    password = "TestPassword123!"
    salt_hex = crypto.generate_salt()
    
    try:
        enc_cmd = crypto.encrypt_pipeline_cmd(password, salt_hex, 10000)
        dec_cmd = crypto.decrypt_pipeline_cmd(password, salt_hex, 10000)
        
        print(f"   Encrypt command: {enc_cmd[:50]}...")
        print(f"   Decrypt command: {dec_cmd[:50]}...")
        print(f"   ✅ Pipeline commands generated")
        return True
    except Exception as e:
        print(f"   ❌ Pipeline command generation failed: {e}")
        return False

def test_metadata():
    """Test metadata save/load"""
    print("\n🔍 Testing metadata save/load...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = os.path.join(tmpdir, "test_archive")
        salt_hex = crypto.generate_salt()
        iterations = 100000
        
        try:
            # Save metadata
            crypto.save_metadata(base_path, salt_hex, iterations)
            metadata_file = base_path + ".enc"
            
            if not os.path.exists(metadata_file):
                print(f"   ❌ Metadata file not created")
                return False
            
            print(f"   ✅ Metadata file created: {os.path.basename(metadata_file)}")
            
            # Load metadata
            loaded = crypto.load_metadata(base_path)
            
            if loaded['salt'] == salt_hex and loaded['iterations'] == iterations:
                print(f"   ✅ Metadata loaded correctly")
                print(f"      Salt: {loaded['salt'][:16]}...")
                print(f"      Iterations: {loaded['iterations']}")
                return True
            else:
                print(f"   ❌ Metadata mismatch")
                return False
                
        except Exception as e:
            print(f"   ❌ Metadata test failed: {e}")
            return False

def test_file_encryption():
    """Test file encryption/decryption"""
    print("\n🔍 Testing file encryption/decryption...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test file
        test_file = os.path.join(tmpdir, "test.txt")
        encrypted_file = os.path.join(tmpdir, "test.txt.enc")
        decrypted_file = os.path.join(tmpdir, "test_decrypted.txt")
        
        test_content = "This is a test file for encryption!\n" * 100
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        print(f"   Created test file: {len(test_content)} bytes")
        
        password = "TestPassword123!"
        salt_hex = crypto.generate_salt()
        iterations = 10000
        
        try:
            # Encrypt
            crypto.encrypt_file(test_file, encrypted_file, password, salt_hex, iterations)
            
            if not os.path.exists(encrypted_file):
                print(f"   ❌ Encrypted file not created")
                return False
            
            enc_size = os.path.getsize(encrypted_file)
            print(f"   ✅ File encrypted: {enc_size} bytes")
            
            # Decrypt
            crypto.decrypt_file(encrypted_file, decrypted_file, password, salt_hex, iterations)
            
            if not os.path.exists(decrypted_file):
                print(f"   ❌ Decrypted file not created")
                return False
            
            # Compare
            with open(decrypted_file, 'r') as f:
                decrypted_content = f.read()
            
            if decrypted_content == test_content:
                print(f"   ✅ Decryption successful - content matches!")
                return True
            else:
                print(f"   ❌ Decrypted content doesn't match original")
                return False
                
        except Exception as e:
            print(f"   ❌ File encryption test failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("  Encryption Module Test Suite")
    print("=" * 60)
    
    tests = [
        ("OpenSSL Check", test_openssl),
        ("Salt Generation", test_salt_generation),
        ("Key Derivation", test_key_derivation),
        ("Pipeline Commands", test_pipeline_commands),
        ("Metadata Save/Load", test_metadata),
        ("File Encryption/Decryption", test_file_encryption),
    ]
    
    results = []
    for name, test_func in tests:
        result = test_func()
        results.append((name, result))
    
    # Summary
    print("\n" + "=" * 60)
    print("  Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\n  Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  🎉 All tests passed!")
        return 0
    else:
        print(f"\n  ⚠️  {total - passed} test(s) failed")
        return 1

if __name__ == '__main__':
    sys.exit(main())
