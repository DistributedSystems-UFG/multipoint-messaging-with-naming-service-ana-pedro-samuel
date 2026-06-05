from socket import *
import pickle
from constMP import *


class GroupManager:
    def __init__(self):
        self.membership: list[tuple[str, int]] = []
        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', GROUPMNGR_TCP_PORT))
        self.server_sock.listen(6)

    def _handle_register(self, req: dict, conn):
        peer = (req["ipaddr"], req["port"])
        if peer not in self.membership:
            self.membership.append(peer)
        print(f"[GroupManager] Registered peer: {peer} | total={len(self.membership)}")

    def _handle_list(self, conn):
        ip_list = [m[0] for m in self.membership]
        print("[GroupManager] Sending peer list to server:", ip_list)
        conn.sendall(pickle.dumps(ip_list))

    def _handle_unregister(self, req: dict):
        peer = (req["ipaddr"], req["port"])
        if peer in self.membership:
            self.membership.remove(peer)
            print(f"[GroupManager] Unregistered peer: {peer}")

    def _handle_stop(self) -> bool:
        print("[GroupManager] Stopping.")
        self.server_sock.close()
        return True

    def _handle_unknown(self, conn):
        print("[GroupManager] Unknown request received.")

    def _dispatch(self, req: dict, conn) -> bool:
        op = req.get("op")
        if op == "register":
            self._handle_register(req, conn)
        elif op == "list":
            self._handle_list(conn)
        elif op == "unregister":
            self._handle_unregister(req)
        elif op == "stop":
            return self._handle_stop()
        else:
            self._handle_unknown(conn)
        return False

    def run(self):
        print(f"[GroupManager] Listening on port {GROUPMNGR_TCP_PORT}")
        while True:
            conn, addr = self.server_sock.accept()
            try:
                raw = conn.recv(2048)
                req = pickle.loads(raw)
                should_stop = self._dispatch(req, conn)
            finally:
                conn.close()

            if should_stop:
                break


if __name__ == "__main__":
    manager = GroupManager()
    manager.run()