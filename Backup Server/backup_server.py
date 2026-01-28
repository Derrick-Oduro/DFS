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

# ---------------- REPLICATION HANDLER ----------------
def handle_replication(client_socket, addr):
    print(f"[+] Replication request from: {addr}")
    
    try:
        command = client_socket.recv(1024).decode().strip()
        
        # -------- REPLICATE WRITE --------
        if command.startswith("REPLICATE "):
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
        
        # -------- REPLICATE BINARY FILES --------
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
        
        # -------- READ FROM BACKUP --------
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
            print(f"[+] Served file from backup: {filename}")
        
        # -------- DELETE FROM BACKUP --------
        elif command.startswith("DELETE"):
            _, filename = command.split()
            filepath = os.path.join(STORAGE_DIR, filename)
            
            if not os.path.exists(filepath):
                client_socket.send("ERROR: File not found".encode())
                return
            
            lock = get_lock(filename)
            with lock:
                os.remove(filepath)
            
            client_socket.send("Delete successful".encode())
            print(f"[+] Deleted file from backup: {filename}")
        
        # -------- LIST BACKUP FILES --------
        elif command == "LIST":
            files = os.listdir(STORAGE_DIR)
            client_socket.send(str(files).encode())
        
        else:
            client_socket.send("ERROR: Invalid command".encode())
    
    except Exception as e:
        print(f"[!] Error: {str(e)}")
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

print(f"Backup Server running on port {BACKUP_PORT}...")
print(f"Storage directory: {STORAGE_DIR}")

while True:
    try:
        client_socket, addr = backup_server.accept()
        replication_thread = threading.Thread(
            target=handle_replication,
            args=(client_socket, addr)
        )
        replication_thread.daemon = True
        replication_thread.start()
    except KeyboardInterrupt:
        print("\n[!] Backup server shutting down...")
        break
    except Exception as e:
        print(f"[!] Server error: {str(e)}")

backup_server.close()