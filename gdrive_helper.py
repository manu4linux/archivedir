#!/usr/bin/env python3
"""
Google Drive Helper - Authentication and streaming file operations
Provides methods to authenticate, create folders, upload/download files in streaming mode
"""

import os
import io
import threading
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Scopes required for Drive file operations
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Credential files
CREDENTIALS_FILE = 'client_secret_apps.googleusercontent.com.json'
TOKEN_FILE = 'gdrive_token.json'


def authenticate():
    """
    Authenticate and return Google Drive service.
    
    Returns:
        googleapiclient.discovery.Resource: Authenticated Drive service
    """
    creds = None
    
    # Token stores user's access and refresh tokens
    if os.path.exists(TOKEN_FILE):
        print(f"📝 Loading credentials from {TOKEN_FILE}")
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # If no valid creds, start OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Refreshing expired credentials...")
            creds.refresh(Request())
        else:
            print(f"🔐 Starting OAuth flow (opens browser)...")
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"Credentials file not found: {CREDENTIALS_FILE}\n"
                    f"Please download OAuth credentials from Google Cloud Console"
                )
            
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save token for future use
        print(f"💾 Saving credentials to {TOKEN_FILE}")
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    
    print("✅ Authentication successful!")
    return build('drive', 'v3', credentials=creds)


def create_folder(service, folder_name, parent_id=None):
    """
    Create a folder in Google Drive.
    
    Args:
        service: Authenticated Drive service
        folder_name (str): Name of the folder to create
        parent_id (str, optional): Parent folder ID. If None, creates in root
    
    Returns:
        str: ID of the created folder
    """
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        print(f"📁 Creating folder: {folder_name}")
        folder = service.files().create(
            body=file_metadata,
            fields='id, name, webViewLink'
        ).execute()
        
        print(f"✅ Folder created!")
        print(f"   ID: {folder.get('id')}")
        print(f"   Name: {folder.get('name')}")
        print(f"   Link: {folder.get('webViewLink')}")
        
        return folder.get('id')
    
    except HttpError as error:
        print(f"❌ Error creating folder: {error}")
        raise


def find_folder(service, folder_name, parent_id=None):
    """
    Find a folder by name in Google Drive.
    
    Args:
        service: Authenticated Drive service
        folder_name (str): Name of the folder to find
        parent_id (str, optional): Parent folder ID to search in
    
    Returns:
        str: ID of the folder if found, None otherwise
    """
    try:
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        print(f"🔍 Searching for folder: {folder_name}")
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, webViewLink)'
        ).execute()
        
        folders = results.get('files', [])
        
        if folders:
            folder = folders[0]
            print(f"✅ Found folder!")
            print(f"   ID: {folder.get('id')}")
            print(f"   Name: {folder.get('name')}")
            print(f"   Link: {folder.get('webViewLink')}")
            return folder.get('id')
        else:
            print(f"❌ Folder not found: {folder_name}")
            return None
    
    except HttpError as error:
        print(f"❌ Error searching for folder: {error}")
        raise


def get_or_create_folder(service, folder_name, parent_id=None):
    """
    Get folder ID if exists, otherwise create it.
    
    Args:
        service: Authenticated Drive service
        folder_name (str): Name of the folder
        parent_id (str, optional): Parent folder ID
    
    Returns:
        str: ID of the folder
    """
    folder_id = find_folder(service, folder_name, parent_id)
    
    if folder_id:
        return folder_id
    else:
        return create_folder(service, folder_name, parent_id)


def get_or_create_folder_path(service, folder_path, parent_id=None):
    """
    Get or create a nested folder path (e.g., "Backups/2025/backup_123").
    Creates intermediate folders as needed.
    
    Args:
        service: Authenticated Drive service
        folder_path (str): Folder path (e.g., "Backups/2025/backup_123")
        parent_id (str, optional): Parent folder ID to start from
    
    Returns:
        str: ID of the final folder in the path
    """
    path_parts = folder_path.strip('/').split('/')
    current_parent_id = parent_id
    
    for folder_name in path_parts:
        current_parent_id = get_or_create_folder(service, folder_name, current_parent_id)
    
    return current_parent_id


def upload_file_streaming(service, file_stream, filename, folder_id=None, mime_type='application/octet-stream', chunk_size_mb=10):
    """
    Upload a file to Google Drive using streaming (resumable upload).
    Shows progress in a separate thread.
    
    Args:
        service: Authenticated Drive service
        file_stream: File-like object (readable stream)
        filename (str): Name for the file in Drive
        folder_id (str, optional): Parent folder ID
        mime_type (str): MIME type of the file
        chunk_size_mb (int): Chunk size in MB for upload
    
    Returns:
        str: ID of the uploaded file
    """
    try:
        file_metadata = {'name': filename}
        
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        chunk_size = chunk_size_mb * 1024 * 1024  # Convert to bytes
        
        print(f"📤 Uploading file: {filename}")
        print(f"   Folder ID: {folder_id or 'Root'}")
        print(f"   Chunk size: {chunk_size_mb} MB")
        
        media = MediaIoBaseUpload(
            file_stream,
            mimetype=mime_type,
            chunksize=chunk_size,
            resumable=True
        )
        
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, size, webViewLink'
        )
        
        # Shared state for progress tracking
        progress_data = {'progress': 0, 'done': False}
        progress_lock = threading.Lock()
        
        def progress_monitor():
            """Monitor and display upload progress in separate thread"""
            while True:
                with progress_lock:
                    current_progress = progress_data['progress']
                    is_done = progress_data['done']
                
                if is_done:
                    break
                
                print(f"   📊 Progress: {current_progress}%", end='\r', flush=True)
                time.sleep(0.1)  # Update every 100ms
        
        # Start progress monitoring thread
        progress_thread = threading.Thread(target=progress_monitor, daemon=True)
        progress_thread.start()
        
        # Upload with progress tracking
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                with progress_lock:
                    progress_data['progress'] = int(status.progress() * 100)
        
        # Signal completion to progress thread
        with progress_lock:
            progress_data['done'] = True
            progress_data['progress'] = 100
        
        # Wait for progress thread to finish
        progress_thread.join(timeout=1.0)
        
        print()  # New line after progress
        print(f"✅ Upload complete!")
        print(f"   ID: {response.get('id')}")
        print(f"   Name: {response.get('name')}")
        print(f"   Size: {int(response.get('size', 0)) / (1024**2):.2f} MB")
        print(f"   Link: {response.get('webViewLink')}")
        
        return response.get('id')
    
    except HttpError as error:
        # Ensure progress thread is stopped
        with progress_lock:
            progress_data['done'] = True
        print(f"❌ Error uploading file: {error}")
        raise


def download_file_streaming(service, file_id, output_stream):
    """
    Download a file from Google Drive using streaming.
    
    Args:
        service: Authenticated Drive service
        file_id (str): ID of the file to download
        output_stream: File-like object (writable stream)
    
    Returns:
        bool: True if successful
    """
    try:
        # Get file metadata
        file_metadata = service.files().get(
            fileId=file_id,
            fields='name, size'
        ).execute()
        
        filename = file_metadata.get('name')
        file_size = int(file_metadata.get('size', 0))
        
        print(f"📥 Downloading file: {filename}")
        print(f"   Size: {file_size / (1024**2):.2f} MB")
        
        request = service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(output_stream, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                print(f"   📊 Progress: {progress}%", end='\r')
        
        print()  # New line after progress
        print(f"✅ Download complete!")
        return True
    
    except HttpError as error:
        print(f"❌ Error downloading file: {error}")
        raise


def list_files(service, folder_id=None, query=None):
    """
    List files in Google Drive.
    
    Args:
        service: Authenticated Drive service
        folder_id (str, optional): Folder ID to list files from
        query (str, optional): Additional query parameters
    
    Returns:
        list: List of file dictionaries
    """
    try:
        base_query = "trashed=false"
        
        if folder_id:
            base_query += f" and '{folder_id}' in parents"
        
        if query:
            base_query += f" and {query}"
        
        print(f"📋 Listing files...")
        results = service.files().list(
            q=base_query,
            spaces='drive',
            fields='files(id, name, size, mimeType, modifiedTime, webViewLink)',
            pageSize=100
        ).execute()
        
        files = results.get('files', [])
        
        print(f"✅ Found {len(files)} file(s)")
        for i, file in enumerate(files, 1):
            size_mb = int(file.get('size', 0)) / (1024**2) if file.get('size') else 0
            print(f"   [{i}] {file.get('name')} ({size_mb:.2f} MB)")
            print(f"       ID: {file.get('id')}")
        
        return files
    
    except HttpError as error:
        print(f"❌ Error listing files: {error}")
        raise


def delete_file(service, file_id):
    """
    Delete a file from Google Drive.
    
    Args:
        service: Authenticated Drive service
        file_id (str): ID of the file to delete
    
    Returns:
        bool: True if successful
    """
    try:
        # Get file name first
        file_metadata = service.files().get(
            fileId=file_id,
            fields='name'
        ).execute()
        
        filename = file_metadata.get('name')
        
        print(f"🗑️  Deleting file: {filename}")
        service.files().delete(fileId=file_id).execute()
        
        print(f"✅ File deleted!")
        return True
    
    except HttpError as error:
        print(f"❌ Error deleting file: {error}")
        raise


def main():
    """Example usage of Google Drive helper functions."""
    print("=== Google Drive Helper - Example Usage ===\n")
    
    # 1. Authenticate
    service = authenticate()
    
    # 2. Create or get a folder
    folder_id = get_or_create_folder(service, "Test Backups")
    
    # 3. Upload a test file
    test_content = b"Hello, Google Drive! This is a test file."
    test_stream = io.BytesIO(test_content)
    
    file_id = upload_file_streaming(
        service,
        test_stream,
        "test_file.txt",
        folder_id=folder_id,
        mime_type='text/plain'
    )
    
    # 4. List files in the folder
    list_files(service, folder_id=folder_id)
    
    # 5. Download the file
    output_stream = io.BytesIO()
    download_file_streaming(service, file_id, output_stream)
    
    # Verify content
    output_stream.seek(0)
    downloaded_content = output_stream.read()
    print(f"\n📄 Downloaded content: {downloaded_content.decode()}")
    
    # 6. Optional: Delete the test file
    # delete_file(service, file_id)
    
    print("\n✅ All operations completed successfully!")


if __name__ == '__main__':
    main()
