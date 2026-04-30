import socket
import json
import os
import threading
from datetime import datetime

# Configuration
SERVER_HOST = 'localhost'
SERVER_PORT = 5000
FILES_DIR = 'files'
DEFAULT_USER = 'student'
DEFAULT_PASSWORD = '1234'
FILE_HISTORY = {}
HISTORY_LOCK = threading.Lock()

def ensure_files_dir():
    """Ensure files directory exists"""
    if not os.path.exists(FILES_DIR):
        os.makedirs(FILES_DIR)
            print(f"Directory '{FILES_DIR}' created")


def authenticate(username, password):
    """Authenticate user"""
    return username == DEFAULT_USER and password == DEFAULT_PASSWORD


def record_history(filename, action, detail=None):
    """Record a file operation in history"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    entry = f"{timestamp} - {action}"
    if detail:
        entry = f"{entry}: {detail}"
    with HISTORY_LOCK:
        FILE_HISTORY.setdefault(filename, []).append(entry)


def get_history(filename):
    """Get history list for a file"""
    with HISTORY_LOCK:
        return list(FILE_HISTORY.get(filename, []))


def handle_client(conn, addr):
    """Handle client connection"""
        print(f"\nClient connected from {addr}")
    authenticated = False
    current_user = None
    
    try:
        while True:
            # Receive request
            request_data = conn.recv(4096).decode('utf-8')
            if not request_data:
                break
            
            try:
                request = json.loads(request_data)
                command = request.get('command')
                
                    print(f"Command received: {command}")
                
                # Authentication
                if command == 'login':
                    username = request.get('username')
                    password = request.get('password')
                    
                    if authenticate(username, password):
                        authenticated = True
                        current_user = username
                        response = {'status': 'success', 'message': f'Welcome {username}!'}
                            print(f"User {username} authenticated")
                    else:
                        response = {'status': 'error', 'message': 'Invalid credentials'}
                            print(f"Authentication failed for user {username}")
                
                elif not authenticated:
                    response = {'status': 'error', 'message': 'Not authenticated. Use login first'}
                
                # File operations
                elif command == 'create_file':
                    filename = request.get('filename')
                    content = request.get('content', '')
                    
                    filepath = os.path.join(FILES_DIR, filename)
                    with open(filepath, 'w') as f:
                        f.write(content)
                    record_history(filename, 'create')
                    response = {'status': 'success', 'message': f'File {filename} created on server'}
                        print(f"File created: {filename}")
                
                elif command == 'upload':
                    filename = request.get('filename')
                    content = request.get('content')
                    
                    filepath = os.path.join(FILES_DIR, filename)
                    with open(filepath, 'w') as f:
                        f.write(content)
                    record_history(filename, 'upload')
                    response = {'status': 'success', 'message': f'File {filename} uploaded'}
                        print(f"File uploaded: {filename}")
                
                elif command == 'rename_file':
                    old_name = request.get('old_name')
                    new_name = request.get('new_name')

                    if not old_name or not new_name:
                        response = {'status': 'error', 'message': 'Missing old_name or new_name'}
                    else:
                        old_path = os.path.join(FILES_DIR, old_name)
                        new_path = os.path.join(FILES_DIR, new_name)

                        if not os.path.exists(old_path):
                            response = {'status': 'error', 'message': f'File {old_name} not found'}
                        elif os.path.exists(new_path):
                            response = {'status': 'error', 'message': f'File {new_name} already exists'}
                        else:
                            os.rename(old_path, new_path)
                            with HISTORY_LOCK:
                                history = FILE_HISTORY.pop(old_name, [])
                                FILE_HISTORY[new_name] = history
                            record_history(new_name, 'rename', f'{old_name} -> {new_name}')
                            response = {'status': 'success', 'message': f'File renamed to {new_name}'}
                                print(f"File renamed: {old_name} -> {new_name}")
                
                elif command == 'read_file':
                    filename = request.get('filename')
                    if not filename:
                        response = {'status': 'error', 'message': 'Missing filename'}
                    else:
                        filepath = os.path.join(FILES_DIR, filename)
                        if not os.path.exists(filepath):
                            response = {'status': 'error', 'message': f'File {filename} not found'}
                        else:
                            with open(filepath, 'r') as f:
                                content = f.read()
                            record_history(filename, 'read')
                            response = {'status': 'success', 'filename': filename, 'content': content}
                
                elif command == 'download':
                    filename = request.get('filename')
                    if not filename:
                        response = {'status': 'error', 'message': 'Missing filename'}
                    else:
                        filepath = os.path.join(FILES_DIR, filename)
                        if not os.path.exists(filepath):
                            response = {'status': 'error', 'message': f'File {filename} not found'}
                        else:
                            with open(filepath, 'r') as f:
                                content = f.read()
                            record_history(filename, 'download')
                            response = {'status': 'success', 'filename': filename, 'content': content}
                
                elif command == 'edit_file':
                    filename = request.get('filename')
                    content = request.get('content')
                    if not filename:
                        response = {'status': 'error', 'message': 'Missing filename'}
                    else:
                        filepath = os.path.join(FILES_DIR, filename)
                        if not os.path.exists(filepath):
                            response = {'status': 'error', 'message': f'File {filename} not found'}
                        else:
                            with open(filepath, 'w') as f:
                                f.write(content or '')
                            record_history(filename, 'edit')
                            response = {'status': 'success', 'message': f'File {filename} updated'}
                
                elif command == 'see_file_operation_history':
                    filename = request.get('filename')
                    if not filename:
                        response = {'status': 'error', 'message': 'Missing filename'}
                    else:
                        filepath = os.path.join(FILES_DIR, filename)
                        if not os.path.exists(filepath):
                            response = {'status': 'error', 'message': f'File {filename} not found'}
                        else:
                            history = get_history(filename)
                            response = {
                                'status': 'success',
                                'filename': filename,
                                'history': history
                            }
                
                elif command == 'list_files':
                    files = os.listdir(FILES_DIR)
                    response = {'status': 'success', 'files': files}
                        print(f"Files listed: {len(files)} files found")
                
                elif command == 'logout':
                    authenticated = False
                    current_user = None
                    response = {'status': 'success', 'message': 'Logged out'}
                        print(f"User logged out")
                
                else:
                    response = {'status': 'error', 'message': f'Unknown command: {command}'}
                
            except Exception as e:
                response = {'status': 'error', 'message': str(e)}
                    print(f"Error: {str(e)}")
            
            # Send response
            conn.send(json.dumps(response).encode('utf-8'))
    
    except Exception as e:
            print(f"Connection error: {str(e)}")
    finally:
        conn.close()
            print(f"Client disconnected from {addr}")


def start_server():
    """Start FTP server"""
    ensure_files_dir()
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen(5)
    
    print("=" * 60)
        print("FTP SERVER STARTED")
    print("=" * 60)
    print(f"Host: {SERVER_HOST}")
    print(f"Port: {SERVER_PORT}")
    print(f"Files Directory: {FILES_DIR}")
    print(f"Default User: {DEFAULT_USER}")
    print(f"Default Password: {DEFAULT_PASSWORD}")
    print("=" * 60)
    
    try:
        while True:
            conn, addr = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.daemon = True
            client_thread.start()
    except KeyboardInterrupt:
            print("\n\nServer shutting down...")
    finally:
        server_socket.close()


if __name__ == '__main__':
    start_server()
