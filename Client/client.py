import socket
import time
import os
import json

SERVER_IP = "127.0.0.1"
PORT = 9000
BACKUP_SERVER_IP = "127.0.0.1"
BACKUP_PORT = 9001
CACHE_DIR = "cache"
CACHE_FILE = os.path.join(CACHE_DIR, "file_cache.json")

# Create cache directory
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------------- CACHE MANAGEMENT ----------------
def load_cache():
    """Load cache from disk"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    """Save cache to disk"""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

def get_from_cache(filename):
    """Get file content from cache"""
    cache = load_cache()
    if filename in cache:
        print(f"[CACHE HIT] Retrieved {filename} from cache")
        return cache[filename]["content"]
    return None

def add_to_cache(filename, content):
    """Add file content to cache"""
    cache = load_cache()
    cache[filename] = {
        "content": content,
        "timestamp": time.time()
    }
    save_cache(cache)
    print(f"[CACHE] Added {filename} to cache")

def invalidate_cache(filename):
    """Remove file from cache"""
    cache = load_cache()
    if filename in cache:
        del cache[filename]
        save_cache(cache)
        print(f"[CACHE] Invalidated {filename}")

# ---------------- NETWORK OPERATIONS ----------------
def send_command(command, data=None, use_backup=False):
    """Send command to server with automatic failover"""
    server_ip = BACKUP_SERVER_IP if use_backup else SERVER_IP
    port = BACKUP_PORT if use_backup else PORT
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(10)
            client.connect((server_ip, port))

            client.send(command.encode())

            if command.startswith("WRITE") or command.startswith("APPEND"):
                response = client.recv(1024).decode()
                if response == "READY":
                    client.send(data.encode())
                    client.send("<<EOF>>".encode())

            result = client.recv(4096).decode()
            client.close()
            
            # Cache READ results
            if command.startswith("READ") and not result.startswith("ERROR"):
                filename = command.split()[1]
                add_to_cache(filename, result)
            
            # Invalidate cache on WRITE/UPLOAD/APPEND/DELETE
            if command.startswith(("WRITE", "UPLOAD", "APPEND", "DELETE")):
                filename = command.split()[1]
                invalidate_cache(filename)
            
            return result
        
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            print(f"[!] Connection attempt {attempt + 1} failed: {str(e)}")
            
            if not use_backup and command.startswith("READ") and attempt == max_retries - 1:
                print("[!] Trying backup server...")
                return send_command(command, data, use_backup=True)
            
            if command.startswith("READ") and attempt == max_retries - 1:
                filename = command.split()[1]
                cached_content = get_from_cache(filename)
                if cached_content:
                    return f"[FROM CACHE]\n{cached_content}"
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        
        except Exception as e:
            print(f"[!] Error: {str(e)}")
            return f"ERROR: {str(e)}"
        
        finally:
            try:
                client.close()
            except:
                pass
    
    return "ERROR: Could not connect to server after multiple attempts"

def upload_file(filepath):
    """Upload a file to the server"""
    if not os.path.exists(filepath):
        return "ERROR: File not found on local system"
    
    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(30)
            client.connect((SERVER_IP, PORT))
            
            # Send upload command
            client.send(f"UPLOAD {filename} {filesize}".encode())
            response = client.recv(1024).decode()
            
            if response == "READY":
                # Send file data
                with open(filepath, "rb") as f:
                    sent = 0
                    while sent < filesize:
                        chunk = f.read(4096)
                        client.send(chunk)
                        sent += len(chunk)
                
                result = client.recv(1024).decode()
                client.close()
                invalidate_cache(filename)
                return result
            else:
                client.close()
                return f"ERROR: {response}"
        
        except Exception as e:
            print(f"[!] Upload attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)
        finally:
            try:
                client.close()
            except:
                pass
    
    return "ERROR: Could not upload file after multiple attempts"

def download_file(filename, save_path=None):
    """Download a file from the server"""
    if save_path is None:
        save_path = filename
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(30)
            client.connect((SERVER_IP, PORT))
            
            client.send(f"DOWNLOAD {filename}".encode())
            response = client.recv(1024).decode()
            
            if response.startswith("READY"):
                _, filesize = response.split()
                filesize = int(filesize)
                
                client.send("ACK".encode())
                
                # Receive file data
                received = 0
                file_data = b""
                while received < filesize:
                    chunk = client.recv(min(4096, filesize - received))
                    if not chunk:
                        break
                    file_data += chunk
                    received += len(chunk)
                
                # Save to local file
                with open(save_path, "wb") as f:
                    f.write(file_data)
                
                client.close()
                return f"Download successful: {filename} ({filesize} bytes) saved to {save_path}"
            else:
                client.close()
                return response
        
        except Exception as e:
            print(f"[!] Download attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)
        finally:
            try:
                client.close()
            except:
                pass
    
    return "ERROR: Could not download file after multiple attempts"

# ---------------- CLIENT INTERFACE ----------------
print("=== Distributed File System Client ===")
print("Features: Caching, Automatic Failover, Replication, File Transfer\n")

while True:
    print("\nCommands:")
    print("  LIST                    - List files on server")
    print("  READ <file>             - Read file content")
    print("  WRITE <file>            - Overwrite/create file with text")
    print("  APPEND <file>           - Add text to end of existing file")
    print("  DELETE <file>           - Delete file from server")
    print("  UPLOAD <local_path>     - Upload file to server")
    print("  DOWNLOAD <file> [path]  - Download file from server")
    print("  CACHE                   - Show cached files")
    print("  EXIT                    - Exit client")
    
    cmd = input("\n>> ").strip()

    if cmd == "EXIT":
        break
    
    elif cmd == "CACHE":
        cache = load_cache()
        if cache:
            print("\n--- Cached Files ---")
            for filename, info in cache.items():
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(info['timestamp']))
                print(f"  {filename} (cached at {timestamp})")
        else:
            print("Cache is empty")

    elif cmd.startswith("WRITE"):
        try:
            _, filename = cmd.split()
            content = input("Enter file content:\n")
            print(send_command(cmd, content))
        except ValueError:
            print("Usage: WRITE <filename>")
    
    elif cmd.startswith("APPEND"):
        try:
            _, filename = cmd.split()
            content = input("Enter content to append:\n")
            print(send_command(cmd, content))
        except ValueError:
            print("Usage: APPEND <filename>")
    
    elif cmd.startswith("DELETE"):
        try:
            _, filename = cmd.split()
            confirm = input(f"Are you sure you want to delete '{filename}'? (yes/no): ")
            if confirm.lower() == "yes":
                print(send_command(cmd))
            else:
                print("Delete cancelled")
        except ValueError:
            print("Usage: DELETE <filename>")
    
    elif cmd.startswith("UPLOAD"):
        try:
            parts = cmd.split(maxsplit=1)
            if len(parts) < 2:
                print("Usage: UPLOAD <local_file_path>")
            else:
                filepath = parts[1].strip('"')
                print(upload_file(filepath))
        except Exception as e:
            print(f"ERROR: {str(e)}")
    
    elif cmd.startswith("DOWNLOAD"):
        try:
            parts = cmd.split()
            if len(parts) < 2:
                print("Usage: DOWNLOAD <filename> [save_path]")
            else:
                filename = parts[1]
                save_path = parts[2] if len(parts) > 2 else filename
                print(download_file(filename, save_path))
        except Exception as e:
            print(f"ERROR: {str(e)}")

    else:
        print(send_command(cmd))
