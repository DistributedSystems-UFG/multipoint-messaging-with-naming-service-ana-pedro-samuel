from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
import os
import pickle
import random
import threading
import time

from constMP import DB_FILE_PREFIX, PEER_TCP_PORT, PEER_UDP_PORT, PEER_TYPE, SERVER_NAME
from namingService import NamingServiceClient, compose_endpoint, detect_local_ip, split_endpoint


class MessageHandler(threading.Thread):
    def __init__(self, recv_socket, myself, expected_handshakes, total_expected_messages):
        threading.Thread.__init__(self)
        self.sock = recv_socket
        self.myself = myself

        self.expected_handshakes = expected_handshakes
        self.total_expected_messages = total_expected_messages

        self.handshake_count = 0
        self._lock = threading.Lock()

        self.db_file = "%s%s.txt" % (DB_FILE_PREFIX, self.myself)
        self.db = {}
        self.applied_log = []

        self.buffer = {}
        self.next_expected_seq = 1
        self.finished = False

        self._load_or_create_db()

    def increment_handshake(self):
        with self._lock:
            self.handshake_count += 1

    def get_handshake_count(self):
        with self._lock:
            return self.handshake_count

    def _load_or_create_db(self):
        if os.path.exists(self.db_file):
            self._load_db()
        else:
            self.db = {
                "registro_%d" % i: "valor_inicial_%d" % i
                for i in range(1, 101)
            }
            self._save_db()
            print("[Peer %s] Created initial DB with 100 entries in %s" % (self.myself, self.db_file))

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
        print("[Peer %s] Loaded DB from %s with %d entries" % (self.myself, self.db_file, len(self.db)))

    def _save_db(self):
        with open(self.db_file, "w", encoding="utf-8") as f:
            for key in sorted(self.db.keys(), key=lambda k: int(k.split("_")[1])):
                f.write("%s;%s\n" % (key, self.db[key]))

    def _wait_for_handshakes(self):
        print("[Peer %s] Handler ready. Waiting for handshakes..." % self.myself)
        while self.get_handshake_count() < self.expected_handshakes:
            raw = self.sock.recv(1024)
            msg = pickle.loads(raw)

            if isinstance(msg, tuple) and len(msg) >= 2 and msg[0] == "READY":
                self.increment_handshake()
                print(
                    "[Peer %s] Handshake received from peer %s (%d/%d)"
                    % (
                        self.myself,
                        msg[1],
                        self.get_handshake_count(),
                        self.expected_handshakes,
                    )
                )

        print("[Peer %s] All handshakes received. Entering ordered receive loop." % self.myself)

    def _apply_write(self, key, value):
        self.db[key] = value
        self._save_db()

    def _apply_read(self, key):
        return self.db.get(key, "<missing>")

    def _apply_message(self, msg):
        seq = msg["seq"]
        kind = msg["kind"]
        origin = msg["from"]
        key = msg["key"]
        value = msg.get("value")

        if kind == "WRITE":
            self._apply_write(key, value)
            text = "[Peer %s] Applied seq=%s from peer %s: WRITE %s=%s" % (
                self.myself,
                seq,
                origin,
                key,
                value,
            )
        elif kind == "READ":
            current = self._apply_read(key)
            text = "[Peer %s] Applied seq=%s from peer %s: READ %s -> %s" % (
                self.myself,
                seq,
                origin,
                key,
                current,
            )
        elif kind == "END":
            text = "[Peer %s] Applied seq=%s: END marker received" % (self.myself, seq)
            self.finished = True
        else:
            text = "[Peer %s] Applied seq=%s: unknown kind=%s" % (self.myself, seq, kind)

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
        print("[Peer %s] Waiting for ordered operations from sequencer..." % self.myself)
        while not self.finished:
            raw = self.sock.recv(4096)
            msg = pickle.loads(raw)

            if isinstance(msg, dict) and msg.get("op") == "apply":
                seq = msg["seq"]
                self.buffer[seq] = msg
                print("[Peer %s] Buffered seq=%s (%s from peer %s)" % (
                    self.myself,
                    seq,
                    msg["kind"],
                    msg["from"],
                ))
                self._process_buffer()
            else:
                print("[Peer %s] Ignored unexpected UDP message: %s" % (self.myself, msg))

    def _write_log_file(self):
        filename = "logfile%s.log" % self.myself
        with open(filename, "w", encoding="utf-8") as f:
            for line in self.applied_log:
                f.write(line + "\n")

    def get_snapshot(self):
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
        self.myself = -1
        self.service_name = "peer-%s-%s" % (os.getpid(), str(random.randint(100000, 999999)))
        self.local_ip = detect_local_ip()

        self.peers = []
        self.msg_handler = None
        self.n_ops = 0
        self.expected_peers = 0
        self.current_round_peers = []

        self.send_socket = socket(AF_INET, SOCK_DGRAM)

        self.recv_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.recv_socket.bind(("0.0.0.0", PEER_UDP_PORT))

        self.tcp_server_sock = socket(AF_INET, SOCK_STREAM)
        self.tcp_server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.tcp_server_sock.bind(("0.0.0.0", PEER_TCP_PORT))
        self.tcp_server_sock.listen(1)

    @staticmethod
    def _read_all(sock):
        chunks = []
        while True:
            data = sock.recv(4096)
            if not data:
                break
            chunks.append(data)
        return b"".join(chunks)

    def _register_with_naming_service(self):
        endpoint = compose_endpoint(self.local_ip, PEER_TCP_PORT)
        self.naming_client.bind(self.service_name, endpoint)
        self.naming_client.register(self.service_name, PEER_TYPE)
        print("[Peer] Registered %s at %s" % (self.service_name, endpoint))

    def _get_server_endpoint(self):
        endpoint = self.naming_client.lookup(SERVER_NAME)
        return split_endpoint(endpoint)

    def _wait_for_control(self, expected_phase):
        while True:
            conn, _ = self.tcp_server_sock.accept()
            try:
                raw = conn.recv(4096)
                msg = pickle.loads(raw)

                if isinstance(msg, dict) and msg.get("op") == "control":
                    phase = msg.get("phase")

                    if phase == "stop":
                        conn.sendall(pickle.dumps({"status": "ok", "phase": "stop"}))
                        return msg

                    if phase == expected_phase:
                        conn.sendall(pickle.dumps({"status": "ok", "phase": phase}))
                        return msg

                conn.sendall(pickle.dumps({"status": "ignored"}))
            finally:
                conn.close()

    def _load_round_configuration(self):
        prepare_msg = self._wait_for_control("prepare")
        if prepare_msg.get("phase") == "stop":
            return None

        self.myself = int(prepare_msg["peer_id"])
        self.n_ops = int(prepare_msg["n_ops"])
        self.expected_peers = int(prepare_msg["expected_peers"])
        self.current_round_peers = prepare_msg["peers"]

        print(
            "[Peer %s] Prepared for round with %s operations per peer and %s total peers."
            % (self.myself, self.n_ops, self.expected_peers)
        )

        go_msg = self._wait_for_control("go")
        if go_msg.get("phase") == "stop":
            return None

        return True

    def send_handshakes(self):
        msg = pickle.dumps(("READY", self.myself))
        for addr in self.peers:
            host, port = split_endpoint(addr)
            print("[Peer %s] Sending handshake to %s" % (self.myself, addr))
            self.send_socket.sendto(msg, (host, port))

    def wait_for_all_handshakes(self):
        while self.msg_handler.get_handshake_count() < len(self.peers):
            time.sleep(0.05)

    def _build_operation(self, local_seq):
        key = "registro_%d" % random.randint(1, 100)

        if random.random() < 0.5:
            return {
                "kind": "WRITE",
                "key": key,
                "value": "peer%s_valor%s_%s" % (self.myself, local_seq, random.randint(1, 9999)),
            }

        return {
            "kind": "READ",
            "key": key,
        }

    def _submit_operation(self, operation, local_seq):
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
            "[Peer %s] Submitting local_seq=%s: %s %s%s"
            % (
                self.myself,
                local_seq,
                req["kind"],
                req["key"],
                ("=%s" % req["value"]) if req["kind"] == "WRITE" else "",
            )
        )

        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect((server_host, server_port))
            sock.sendall(pickle.dumps(req))
            sock.shutdown(1)
            ack = pickle.loads(self._read_all(sock))
            print("[Peer %s] Sequencer ack: %s" % (self.myself, ack))

    def send_operations(self, n_ops):
        for local_seq in range(1, n_ops + 1):
            time.sleep(random.randrange(10, 100) / 1000.0)
            operation = self._build_operation(local_seq)
            self._submit_operation(operation, local_seq)

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

        print("[Peer %s] Sending final replica state to server..." % self.myself)
        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect((server_host, server_port))
            sock.sendall(pickle.dumps(payload))
            sock.shutdown(1)
            ack = pickle.loads(self._read_all(sock))
            print("[Peer %s] Final-state ack: %s" % (self.myself, ack))

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
                loaded = self._load_round_configuration()
                if loaded is None:
                    print("[Peer] Terminating.")
                    break

                if self.n_ops == 0:
                    print("[Peer %s] Terminating." % self.myself)
                    break

                self.peers = [
                    peer["endereco"]
                    for peer in self.current_round_peers
                    if peer["nome"] != self.service_name
                ]

                expected_handshakes = len(self.peers)
                total_expected_messages = (len(self.peers) * self.n_ops) + 1

                self.msg_handler = MessageHandler(
                    self.recv_socket,
                    self.myself,
                    expected_handshakes,
                    total_expected_messages,
                )
                self.msg_handler.start()
                print("[Peer %s] Receiver thread started." % self.myself)

                self.send_handshakes()
                print(
                    "[Peer %s] Handshakes sent. Current count=%s"
                    % (self.myself, self.msg_handler.get_handshake_count())
                )

                self.wait_for_all_handshakes()

                print("[Peer %s] Starting operation submission." % self.myself)
                self.send_operations(self.n_ops)

                print(
                    "[Peer %s] All local operations submitted. Waiting for ordered execution to finish..."
                    % self.myself
                )
                self.msg_handler.join()

                time.sleep(0.5)

                self.send_final_state_to_server()
        finally:
            self.close()


if __name__ == "__main__":
    peer = PeerCommunicator()
    peer.run()
