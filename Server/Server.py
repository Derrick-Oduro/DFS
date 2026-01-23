import socket
import os
import threading
import time

# ---------------- CONFIG ----------------
SERVER_IP = "0.0.0.0"
PORT = 9000
STORAGE_DIR = "Storage"
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

        # -------- UPLOAD --------
        elif command.startswith("UPLOAD"):
            _, filename, filesize = command.split()
            filesize = int(filesize)
            filepath = os.path.join(STORAGE_DIR, filename)

            lock = get_lock(filename)
            client_socket.send("READY".encode())

            # Receive file data
            received = 0
            file_data = b""
            while received < filesize:
                chunk = client_socket.recv(min(4096, filesize - received))
                if not chunk:
                    break
                file_data += chunk
                received += len(chunk)

            with lock:
                with open(filepath, "wb") as f:
                    f.write(file_data)

            # Replicate to backup server (convert bytes to string for text files, or handle binary)
            threading.Thread(target=replicate_to_backup, args=(filename, file_data.decode('utf-8', errors='ignore'))).start()

            client_socket.send(f"Upload successful: {filename} ({filesize} bytes) - replicated to backup".encode())
            print(f"[+] File uploaded: {filename} ({filesize} bytes) from {addr}")

        # -------- DOWNLOAD --------
        elif command.startswith("DOWNLOAD"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)

            if not os.path.exists(filepath):
                client_socket.send("ERROR: File not found".encode())
                return

            lock = get_lock(filename)
            with lock:
                filesize = os.path.getsize(filepath)
                client_socket.send(f"READY {filesize}".encode())
                
                # Wait for client acknowledgment
                ack = client_socket.recv(1024).decode()
                if ack == "ACK":
                    with open(filepath, "rb") as f:
                        while True:
                            chunk = f.read(4096)
                            if not chunk:
                                break
                            client_socket.send(chunk)
            
            print(f"[+] File downloaded: {filename} ({filesize} bytes) by {addr}")

        # -------- DELETE --------
        elif command.startswith("DELETE"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)

            if not os.path.exists(filepath):
                client_socket.send("ERROR: File not found".encode())
                return

            lock = get_lock(filename)
            with lock:
                os.remove(filepath)
            
            # Delete from backup server too
            try:
                backup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                backup_socket.settimeout(5)
                backup_socket.connect((BACKUP_SERVER_IP, BACKUP_PORT))
                backup_socket.send(f"DELETE {filename}".encode())
                backup_response = backup_socket.recv(1024).decode()
                backup_socket.close()
                print(f"[+] File deleted from backup: {filename}")
            except Exception as e:
                print(f"[!] Failed to delete from backup: {str(e)}")
            
            client_socket.send("Delete successful (removed from main and backup)".encode())
            print(f"[+] File deleted: {filename} by {addr}")

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
print(f"Storage directory: {STORAGE_DIR}")
print(f"Listening on: {SERVER_IP}:{PORT}")
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
