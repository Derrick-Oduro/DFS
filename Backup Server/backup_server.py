import socket
import os
import threading

# ---------------- CONFIG ----------------
BACKUP_SERVER_IP = "0.0.0.0"
BACKUP_PORT = 9001
STORAGE_DIR = "Storage"

os.makedirs(STORAGE_DIR, exist_ok=True)

file_locks = {}

def get_lock(filename):
    if filename not in file_locks:
        file_locks[filename] = threading.Lock()
    return file_locks[filename]

# ---------------- CLIENT/REPLICATION HANDLER ----------------
def handle_request(client_socket, addr):
    print(f"[+] Connection from: {addr}")
    
    try:
        command = client_socket.recv(1024).decode().strip()
        
        # -------- LIST --------
        if command == "LIST":
            files = os.listdir(STORAGE_DIR)
            client_socket.send(str(files).encode())
            print(f"[+] Served LIST to: {addr}")
        
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
            print(f"[+] Served READ {filename} to: {addr}")
        
        # -------- WRITE --------
        elif command.startswith("WRITE"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)

            lock = get_lock(filename)
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

            client_socket.send("Write successful (backup server)".encode())
            print(f"[+] WRITE {filename} from: {addr}")
        
        # -------- APPEND --------
        elif command.startswith("APPEND"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)

            lock = get_lock(filename)
            client_socket.send("READY".encode())

            data = ""
            while True:
                chunk = client_socket.recv(4096).decode()
                if chunk == "<<EOF>>":
                    break
                data += chunk

            with lock:
                with open(filepath, "a") as f:
                    f.write(data)

            client_socket.send("Append successful (backup server)".encode())
            print(f"[+] APPEND {filename} from: {addr}")
        
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
            
            client_socket.send("Delete successful (backup server)".encode())
            print(f"[+] DELETE {filename} from: {addr}")
        
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

            client_socket.send(f"Upload successful (backup server): {filename} ({filesize} bytes)".encode())
            print(f"[+] UPLOAD {filename} ({filesize} bytes) from: {addr}")
        
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
            
            print(f"[+] DOWNLOAD {filename} ({filesize} bytes) to: {addr}")
        
        # -------- REPLICATE (from main server) --------
        elif command.startswith("REPLICATE "):
            _, filename = command.split(maxsplit=1)
            filepath = os.path.join(STORAGE_DIR, filename)
            
            lock = get_lock(filename)
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
            
            client_socket.send("Replication successful".encode())
            print(f"[+] Replicated file: {filename}")
        
        # -------- REPLICATE BINARY (from main server) --------
        elif command.startswith("REPLICATE_BINARY"):
            parts = command.split()
            filename = parts[1]
            filesize = int(parts[2])
            filepath = os.path.join(STORAGE_DIR, filename)
            
            lock = get_lock(filename)
            client_socket.send("READY".encode())
            
            # Receive binary data
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
            
            client_socket.send("Binary replication successful".encode())
            print(f"[+] Replicated binary file: {filename} ({filesize} bytes)")
        
        else:
            client_socket.send("ERROR: Invalid command".encode())
    
    except Exception as e:
        print(f"[!] Error handling request from {addr}: {str(e)}")
        try:
            client_socket.send(f"ERROR: {str(e)}".encode())
        except:
            pass
    
    finally:
        client_socket.close()
        print(f"[-] Connection closed: {addr}")

# ---------------- SERVER SETUP ----------------
backup_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
backup_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
backup_server.bind((BACKUP_SERVER_IP, BACKUP_PORT))
backup_server.listen(5)

print("=" * 50)
print("BACKUP SERVER RUNNING")
print("=" * 50)
print(f"Port: {BACKUP_PORT}")
print(f"Storage: {STORAGE_DIR}")
print(f"Functions:")
print("  • Receives replications from Main Server")
print("  • Serves clients when Main Server is down")
print("  • Supports all file operations")
print("=" * 50)

while True:
    try:
        client_socket, addr = backup_server.accept()
        request_thread = threading.Thread(
            target=handle_request,
            args=(client_socket, addr)
        )
        request_thread.daemon = True
        request_thread.start()
    except KeyboardInterrupt:
        print("\n[!] Backup server shutting down...")
        break
    except Exception as e:
        print(f"[!] Server error: {str(e)}")

backup_server.close()