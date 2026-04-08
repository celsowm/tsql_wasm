import socket
import threading
import datetime
import struct

def log_packet(direction, data):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"\n[{timestamp}] {'=>' if direction == 'C2S' else '<='} {direction}: {len(data)} bytes", flush=True)
    
    # Very basic attempt to decode UTF-16LE text from the payload
    # TDS packets typically have an 8 byte header
    if len(data) > 8:
        payload = data[8:]
        # Look for SQL batch or RPC text in the payload
        try:
            # Most SSMS queries are sent as UTF-16LE
            text = payload.decode('utf-16le', errors='replace')
            # Filter out some garbage characters to find readable SQL
            readable = ''.join(c for c in text if c.isprintable() or c in '\n\r\t')
            if "SELECT" in readable.upper() or "EXEC" in readable.upper() or "DECLARE" in readable.upper():
                print(f"--- Decoded Text ---\n{readable.strip()[:500]}...\n--------------------", flush=True)
        except:
            pass

def forward_data(src, dst, direction):
    try:
        while True:
            data = src.recv(65535)
            if not data:
                break
            log_packet(direction, data)
            dst.sendall(data)
    except Exception as e:
        print(f"[{direction}] Connection closed: {e}", flush=True)
    finally:
        src.close()
        dst.close()

def start_proxy(local_port, remote_host, remote_port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('127.0.0.1', local_port))
    server.listen(5)
    print(f"[*] TDS Proxy listening on 127.0.0.1:{local_port}...")
    print(f"[*] Forwarding to {remote_host}:{remote_port}...")
    print("[*] To use: Connect SSMS to 127.0.0.1,1434 (Uncheck 'Encrypt connection' in Options)")
    
    while True:
        client_sock, addr = server.accept()
        print(f"\n[+] Accepted connection from {addr[0]}:{addr[1]}")
        
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_sock.connect((remote_host, remote_port))
        except Exception as e:
            print(f"[-] Failed to connect to proxy target: {e}")
            client_sock.close()
            continue
            
        threading.Thread(target=forward_data, args=(client_sock, server_sock, "C2S")).start()
        threading.Thread(target=forward_data, args=(server_sock, client_sock, "S2C")).start()

if __name__ == '__main__':
    # Local proxy port 1434
    # Real SQL Server is on 11433 (Podman Azure SQL)
    start_proxy(1434, '127.0.0.1', 11433)
