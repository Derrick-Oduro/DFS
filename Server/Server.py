import socket
import os
import threading
import time

# ---------------- CONFIG ----------------
SERVER_IP = "0.0.0.0"
PORT = 9000
STORAGE_DIR = "storage"
BACKUP_SERVER_IP = "127.0.0.1"
BACKUP_PORT = 9001

os.makedirs(STORAGE_DIR, exist_ok=True)

# File locks (for concurrency control)
file_locks = {}

def get_lock(filename):
    if filename not in file_locks:
        file_locks[filename] = threading.Lock()
    return file_locks[filename]

# ---------------- REPLICATION ----------------
def replicate_to_backup(filename, data):
    """Send file to backup server for replication"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            backup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            backup_socket.settimeout(5)
            backup_socket.connect((BACKUP_SERVER_IP, BACKUP_PORT))
            
            backup_socket.send(f"REPLICATE {filename}".encode())
            response = backup_socket.recv(1024).decode()
            
            if response == "READY":
                backup_socket.send(data.encode())
                backup_socket.send("<<EOF>>".encode())
                result = backup_socket.recv(1024).decode()
                backup_socket.close()
                print(f"[+] File replicated to backup: {filename}")
                return True
        
        except Exception as e:
            print(f"[!] Replication attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        
        finally:
            try:
                backup_socket.close()
            except:
                pass
    
    print(f"[!] Failed to replicate {filename} after {max_retries} attempts")
    return False

# ---------------- CLIENT HANDLER ----------------
def handle_client(client_socket, addr):
    print(f"[+] Client connected: {addr}")

    try:
        # Set timeout for client operations
        client_socket.settimeout(30)
        
        command = client_socket.recv(1024).decode().strip()

        # -------- LIST --------
        if command == "LIST":
            files = os.listdir(STORAGE_DIR)
            client_socket.send(str(files).encode())

        # -------- READ --------
        elif command.startswith("READ"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)

            if not os.path.exists(filepath):
                client_socket.send("ERROR: File not found".encode())
                return

            lock = get_lock(filename)
            with lock:
                with open(filepath, "r") as f:
                    data = f.read()

            client_socket.send(data.encode())

        # -------- WRITE --------
        elif command.startswith("WRITE"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)

            lock = get_lock(filename)

            # Tell client server is ready
            client_socket.send("READY".encode())

            data = ""
            while True:
                chunk = client_socket.recv(4096).decode()
                if chunk == "<<EOF>>":
                    break
                data += chunk

            with lock:
                with open(filepath, "w") as f:
                    f.write(data)

            # Replicate to backup server
            threading.Thread(target=replicate_to_backup, args=(filename, data)).start()

            client_socket.send("Write successful (replicated to backup)".encode())

        else:
            client_socket.send("ERROR: Invalid command".encode())

    except socket.timeout:
        print(f"[!] Client timeout: {addr}")
        try:
            client_socket.send("ERROR: Connection timeout".encode())
        except:
            pass
    
    except ConnectionResetError:
        print(f"[!] Connection reset by client: {addr}")
    
    except Exception as e:
        print(f"[!] Error handling client {addr}: {str(e)}")
        try:
            client_socket.send(f"ERROR: {str(e)}".encode())
        except:
            pass

    finally:
        try:
            client_socket.close()
        except:
            pass
        print(f"[-] Client disconnected: {addr}")

# ---------------- SERVER SETUP ----------------
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((SERVER_IP, PORT))
server.listen(5)

print("DFS Server running...")
print(f"Backup server configured at {BACKUP_SERVER_IP}:{BACKUP_PORT}")

while True:
    try:
        client_socket, addr = server.accept()
        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, addr)
        )
        client_thread.daemon = True
        client_thread.start()
    except KeyboardInterrupt:
        print("\n[!] Server shutting down...")
        break
    except Exception as e:
        print(f"[!] Server error: {str(e)}")

server.close()
