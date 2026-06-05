from __future__ import annotations

from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
import os
import pickle
import random
import threading
import time
import uuid

from constMP import DB_FILE_PREFIX, PEER_TYPE, SERVER_NAME
from namingService import NamingServiceClient, compose_endpoint, detect_local_ip, split_endpoint


class MessageHandler(threading.Thread):
    def __init__(self, recv_socket: socket, myself: int, expected_handshakes: int, total_expected_messages: int):
        super().__init__()
        self.sock = recv_socket
        self.myself = myself

        self.expected_handshakes = expected_handshakes
        self.total_expected_messages = total_expected_messages

        self.handshake_count = 0
        self._lock = threading.Lock()

        self.db_file = f"{DB_FILE_PREFIX}{self.myself}.txt"
        self.db: dict[str, str] = {}
        self.applied_log: list[str] = []

        self.buffer: dict[int, dict] = {}
        self.next_expected_seq = 1
        self.finished = False

        self._load_or_create_db()

    def increment_handshake(self):
        with self._lock:
            self.handshake_count += 1

    def get_handshake_count(self) -> int:
        with self._lock:
            return self.handshake_count

    def _load_or_create_db(self):
        if os.path.exists(self.db_file):
            self._load_db()
        else:
            self.db = {
                f"registro_{i}": f"valor_inicial_{i}"
                for i in range(1, 101)
            }
            self._save_db()
            print(f"[Peer {self.myself}] Created initial DB with 100 entries in {self.db_file}")

    def _load_db(self):
        self.db = {}
        with open(self.db_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if ";" in line:
                    key, value = line.split(";", 1)
                    self.db[key] = value
        print(f"[Peer {self.myself}] Loaded DB from {self.db_file} with {len(self.db)} entries")

    def _save_db(self):
        with open(self.db_file, "w", encoding="utf-8") as f:
            for key in sorted(self.db.keys(), key=lambda k: int(k.split("_")[1])):
                f.write(f"{key};{self.db[key]}\n")

    def _wait_for_handshakes(self):
        print(f"[Peer {self.myself}] Handler ready. Waiting for handshakes...")
        while self.get_handshake_count() < self.expected_handshakes:
            raw = self.sock.recv(1024)
            msg = pickle.loads(raw)

            if isinstance(msg, tuple) and msg[0] == "READY":
                self.increment_handshake()
                print(
                    f"[Peer {self.myself}] Handshake received from peer {msg[1]} "
                    f"({self.get_handshake_count()}/{self.expected_handshakes})"
                )

        print(f"[Peer {self.myself}] All handshakes received. Entering ordered receive loop.")

    def _apply_write(self, key: str, value: str):
        self.db[key] = value
        self._save_db()

    def _apply_read(self, key: str) -> str:
        return self.db.get(key, "<missing>")

    def _apply_message(self, msg: dict):
        seq = msg["seq"]
        kind = msg["kind"]
        origin = msg["from"]
        key = msg["key"]
        value = msg.get("value")

        if kind == "WRITE":
            self._apply_write(key, value)
            text = f"[Peer {self.myself}] Applied seq={seq} from peer {origin}: WRITE {key}={value}"
        elif kind == "READ":
            current = self._apply_read(key)
            text = f"[Peer {self.myself}] Applied seq={seq} from peer {origin}: READ {key} -> {current}"
        elif kind == "END":
            text = f"[Peer {self.myself}] Applied seq={seq}: END marker received"
            self.finished = True
        else:
            text = f"[Peer {self.myself}] Applied seq={seq}: unknown kind={kind}"

        print(text)
        self.applied_log.append(text)

    def _process_buffer(self):
        while self.next_expected_seq in self.buffer:
            msg = self.buffer.pop(self.next_expected_seq)
            self._apply_message(msg)
            self.next_expected_seq += 1

            if self.next_expected_seq > self.total_expected_messages:
                self.finished = True
                break

    def _receive_messages(self):
        print(f"[Peer {self.myself}] Waiting for ordered operations from sequencer...")
        while not self.finished:
            raw = self.sock.recv(4096)
            msg = pickle.loads(raw)

            if isinstance(msg, dict) and msg.get("op") == "apply":
                seq = msg["seq"]
                self.buffer[seq] = msg
                print(f"[Peer {self.myself}] Buffered seq={seq} ({msg['kind']} from peer {msg['from']})")
                self._process_buffer()
            else:
                print(f"[Peer {self.myself}] Ignored unexpected UDP message: {msg}")

    def _write_log_file(self):
        filename = f"logfile{self.myself}.log"
        with open(filename, "w", encoding="utf-8") as f:
            for line in self.applied_log:
                f.write(line + "\n")

    def get_snapshot(self) -> dict:
        return {
            "peer": self.myself,
            "db": dict(self.db),
            "log": list(self.applied_log),
            "db_file": self.db_file,
        }

    def run(self):
        self._wait_for_handshakes()
        self._receive_messages()
        self._write_log_file()


class PeerCommunicator:
    def __init__(self):
        self.naming_client = NamingServiceClient()
        self.myself: int = -1
        self.service_name: str = f"peer-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.local_ip: str = detect_local_ip()
        self.bound_port: int = -1

        self.peers: list[str] = []
        self.msg_handler: MessageHandler | None = None

        self.send_socket = socket(AF_INET, SOCK_DGRAM)

        self.recv_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.tcp_server_sock = socket(AF_INET, SOCK_STREAM)
        self.tcp_server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self._bind_local_sockets()

    def _bind_local_sockets(self):
        for _ in range(20):
            tcp_sock = socket(AF_INET, SOCK_STREAM)
            tcp_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            tcp_sock.bind(("", 0))
            port = tcp_sock.getsockname()[1]

            udp_sock = socket(AF_INET, SOCK_DGRAM)
            udp_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            try:
                udp_sock.bind(("", port))
            except OSError:
                tcp_sock.close()
                udp_sock.close()
                continue

            self.tcp_server_sock = tcp_sock
            self.recv_socket = udp_sock
            self.tcp_server_sock.listen(1)
            self.bound_port = port
            return

        raise RuntimeError("Unable to bind TCP and UDP sockets to the same local port.")

    @staticmethod
    def _read_all(sock) -> bytes:
        chunks: list[bytes] = []
        while True:
            data = sock.recv(4096)
            if not data:
                break
            chunks.append(data)
        return b"".join(chunks)

    def _register_with_naming_service(self):
        endpoint = compose_endpoint(self.local_ip, self.bound_port)
        self.naming_client.bind(self.service_name, endpoint)
        self.naming_client.register(self.service_name, PEER_TYPE)
        print(f"[Peer] Registered {self.service_name} at {endpoint}")

    def _get_server_endpoint(self) -> tuple[str, int]:
        endpoint = self.naming_client.lookup(SERVER_NAME)
        return split_endpoint(endpoint)

    def _get_peer_records(self) -> list[dict[str, str]]:
        peers = self.naming_client.discover(PEER_TYPE)
        peers = [peer for peer in peers if peer["nome"] != self.service_name]
        print("[Peer] Discovered peers:", peers)
        return peers

    def send_handshakes(self):
        msg = pickle.dumps(("READY", self.myself))
        for addr in self.peers:
            host, port = split_endpoint(addr)
            print(f"[Peer {self.myself}] Sending handshake to {addr}")
            self.send_socket.sendto(msg, (host, port))

    def wait_for_all_handshakes(self):
        while self.msg_handler.get_handshake_count() < len(self.peers):
            time.sleep(0.05)

    def _build_operation(self, local_seq: int) -> dict:
        key = f"registro_{random.randint(1, 100)}"

        if random.random() < 0.5:
            return {
                "kind": "WRITE",
                "key": key,
                "value": f"peer{self.myself}_valor{local_seq}_{random.randint(1, 9999)}",
            }

        return {
            "kind": "READ",
            "key": key,
        }

    def _submit_operation(self, operation: dict, local_seq: int):
        server_host, server_port = self._get_server_endpoint()

        req = {
            "op": "submit",
            "from": self.myself,
            "local_seq": local_seq,
            "kind": operation["kind"],
            "key": operation["key"],
        }

        if operation["kind"] == "WRITE":
            req["value"] = operation["value"]

        print(
            f"[Peer {self.myself}] Submitting local_seq={local_seq}: "
            f"{req['kind']} {req['key']}"
            + (f"={req['value']}" if req["kind"] == "WRITE" else "")
        )

        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect((server_host, server_port))
            sock.sendall(pickle.dumps(req))
            sock.shutdown(1)
            ack = pickle.loads(self._read_all(sock))
            print(f"[Peer {self.myself}] Sequencer ack: {ack}")

    def send_operations(self, n_ops: int):
        for local_seq in range(1, n_ops + 1):
            time.sleep(random.randrange(10, 100) / 1000)
            operation = self._build_operation(local_seq)
            self._submit_operation(operation, local_seq)

    def wait_to_start(self) -> tuple[int, int]:
        conn, _ = self.tcp_server_sock.accept()
        raw = conn.recv(1024)
        msg = pickle.loads(raw)
        myself, n_ops = msg[0], msg[1]
        conn.sendall(pickle.dumps(f"Peer process {myself} started."))
        conn.close()
        return myself, n_ops

    def send_final_state_to_server(self):
        if self.msg_handler is None:
            return

        server_host, server_port = self._get_server_endpoint()

        snapshot = self.msg_handler.get_snapshot()
        payload = {
            "op": "final_state",
            "peer": snapshot["peer"],
            "db": snapshot["db"],
            "log": snapshot["log"],
            "db_file": snapshot["db_file"],
        }

        print(f"[Peer {self.myself}] Sending final replica state to server...")
        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect((server_host, server_port))
            sock.sendall(pickle.dumps(payload))
            sock.shutdown(1)
            ack = pickle.loads(self._read_all(sock))
            print(f"[Peer {self.myself}] Final-state ack: {ack}")

    def close(self):
        try:
            self.naming_client.unbind(self.service_name)
        except Exception:
            pass

        try:
            self.tcp_server_sock.close()
        except Exception:
            pass

        try:
            self.recv_socket.close()
        except Exception:
            pass

        try:
            self.send_socket.close()
        except Exception:
            pass

    def run(self):
        self._register_with_naming_service()

        try:
            while True:
                print("[Peer] Waiting for signal to start...")
                self.myself, n_ops = self.wait_to_start()
                print(f"[Peer {self.myself}] Up. ID={self.myself} | ops per peer={n_ops}")

                if n_ops == 0:
                    print(f"[Peer {self.myself}] Terminating.")
                    break

                peer_records = self._get_peer_records()
                self.peers = [peer["endereco"] for peer in peer_records]

                expected_handshakes = len(self.peers)
                total_expected_messages = (len(self.peers) * n_ops) + 1

                self.msg_handler = MessageHandler(
                    self.recv_socket,
                    self.myself,
                    expected_handshakes,
                    total_expected_messages,
                )
                self.msg_handler.start()
                print(f"[Peer {self.myself}] Receiver thread started.")

                self.send_handshakes()
                print(
                    f"[Peer {self.myself}] Handshakes sent. "
                    f"Current count={self.msg_handler.get_handshake_count()}"
                )

                self.wait_for_all_handshakes()

                print(f"[Peer {self.myself}] Starting operation submission.")
                self.send_operations(n_ops)

                print(f"[Peer {self.myself}] All local operations submitted. Waiting for ordered execution to finish...")
                self.msg_handler.join()

                time.sleep(0.5)

                self.send_final_state_to_server()
        finally:
            self.close()


if __name__ == "__main__":
    peer = PeerCommunicator()
    peer.run()