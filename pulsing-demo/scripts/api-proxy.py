import socket, threading, sys

def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data: break
            dst.sendall(data)
    except: pass
    finally:
        try: src.close()
        except: pass
        try: dst.close()
        except: pass

def handle(client):
    try:
        request = client.recv(65536)
        if not request: client.close(); return
        first = request.split(b'\r\n')[0].decode('utf-8','replace')
        parts = first.split(' ')
        if parts[0] == 'CONNECT' and len(parts) >= 2:
            host_port = parts[1]
            if ':' in host_port:
                host, port = host_port.rsplit(':', 1)
                port = int(port)
            else:
                host, port = host_port, 443
            print(f"CONNECT {host}:{port}", flush=True)
            # 级联到本机系统代理（用户 VPN 客户端 7897），
            # 直连出口 IP 被 Cloudflare/OpenAI 地区策略拦截。
            remote = socket.create_connection(('127.0.0.1', 7897), timeout=15)
            remote.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = remote.recv(4096)
                if not chunk: raise RuntimeError("upstream proxy closed")
                resp += chunk
            if b" 200 " not in resp.split(b"\r\n")[0]:
                raise RuntimeError(f"upstream proxy refused: {resp[:120]!r}")
            client.sendall(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            # 客户端可能把 CONNECT 和 TLS ClientHello 合并在同一个包里发出；
            # 解析完 CONNECT 后必须把 buffer 里剩余的字节转发出去，否则握手丢失。
            header_end = request.find(b'\r\n\r\n')
            leftover = request[header_end + 4:] if header_end >= 0 else b''
            if leftover:
                remote.sendall(leftover)
            t1 = threading.Thread(target=pipe, args=(client, remote), daemon=True)
            t2 = threading.Thread(target=pipe, args=(remote, client), daemon=True)
            t1.start(); t2.start()
            return  # 不关 socket, 由 pipe 线程管理
        else:
            client.sendall(b'HTTP/1.1 405\r\n\r\n'); client.close()
    except Exception as e:
        print(f"err: {e}", flush=True)
        try: client.close()
        except: pass

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('127.0.0.1', 3128))
server.listen(20)
print("proxy ready :3128 (thread mode)", flush=True)
while True:
    client, _ = server.accept()
    threading.Thread(target=handle, args=(client,), daemon=True).start()
