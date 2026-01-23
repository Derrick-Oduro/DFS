import socket
import time
import os
import json

# ============ REMOTE CLIENT CONFIGURATION ============
# Replace this IP with the server PC's IP address
SERVER_IP = "172.20.10.2"  # <-- Server's IP Address
PORT = 9000
BACKUP_SERVER_IP = "172.20.10.2"  # <-- Server's IP Address
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

            if command.startswith("WRITE"):
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
            
            # Invalidate cache on WRITE
            if command.startswith("WRITE"):
                filename = command.split()[1]
                invalidate_cache(filename)
            
            return result
        
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            print(f"[!] Connection attempt {attempt + 1} failed: {str(e)}")
            
            # If main server fails and this is a READ, try backup
            if not use_backup and command.startswith("READ") and attempt == max_retries - 1:
                print("[!] Trying backup server...")
                return send_command(command, data, use_backup=True)
            
            # Try cache for READ operations
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

# ---------------- CLIENT INTERFACE ----------------
print("=== Distributed File System - REMOTE CLIENT ===")
print(f"Connecting to server at: {SERVER_IP}:{PORT}")
print(f"Backup server at: {BACKUP_SERVER_IP}:{BACKUP_PORT}")
print("Features: Caching, Automatic Failover, Replication\n")

while True:
    print("\nCommands: LIST, READ <file>, WRITE <file>, CACHE, EXIT")
    cmd = input(">> ").strip()

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

    else:
        print(send_command(cmd))