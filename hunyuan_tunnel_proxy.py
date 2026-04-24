"""Expose the host-local Hunyuan SSH tunnel to the OmniHuman Docker network."""

from __future__ import annotations

import os
import selectors
import socket
import threading


LISTEN_HOST = os.getenv("HUNYUAN_PROXY_LISTEN_HOST", "172.18.0.1")
LISTEN_PORT = int(os.getenv("HUNYUAN_PROXY_LISTEN_PORT", "8894"))
TARGET_HOST = os.getenv("HUNYUAN_PROXY_TARGET_HOST", "127.0.0.1")
TARGET_PORT = int(os.getenv("HUNYUAN_PROXY_TARGET_PORT", "8892"))
BUFFER_SIZE = 1024 * 64


def _pipe(client: socket.socket, upstream: socket.socket) -> None:
    selector = selectors.DefaultSelector()
    sockets = [client, upstream]
    try:
        for sock in sockets:
            sock.setblocking(False)
            selector.register(sock, selectors.EVENT_READ)
        while True:
            events = selector.select(timeout=120)
            if not events:
                break
            for key, _ in events:
                source = key.fileobj
                target = upstream if source is client else client
                try:
                    data = source.recv(BUFFER_SIZE)
                except OSError:
                    return
                if not data:
                    return
                target.sendall(data)
    finally:
        for sock in sockets:
            try:
                selector.unregister(sock)
            except Exception:
                pass
            try:
                sock.close()
            except OSError:
                pass
        selector.close()


def _handle(client: socket.socket) -> None:
    try:
        upstream = socket.create_connection((TARGET_HOST, TARGET_PORT), timeout=10)
    except OSError:
        client.close()
        return
    _pipe(client, upstream)


def main() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(32)
    print(f"Hunyuan proxy listening on {LISTEN_HOST}:{LISTEN_PORT} -> {TARGET_HOST}:{TARGET_PORT}", flush=True)
    while True:
        client, _ = server.accept()
        thread = threading.Thread(target=_handle, args=(client,), daemon=True)
        thread.start()


if __name__ == "__main__":
    main()
