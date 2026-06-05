from __future__ import annotations

from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
import pickle
import time

from constMP import PEER_TYPE, SERVER_NAME, SERVER_TYPE
from namingService import NamingServiceClient, compose_endpoint, detect_local_ip, split_endpoint


def _read_all(sock) -> bytes:
    chunks: list[bytes] = []
    while True:
        data = sock.recv(4096)
        if not data:
            break
        chunks.append(data)
    return b"".join(chunks)


class ComparisonServer:
    def __init__(self):
        self.naming_client = NamingServiceClient()
        self.local_ip = detect_local_ip()

        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind(("", 0))
        self.server_sock.listen(16)
        self.local_port = self.server_sock.getsockname()[1]
        self.endpoint = compose_endpoint(self.local_ip, self.local_port)

        self.naming_client.bind(SERVER_NAME, self.endpoint)
        self.naming_client.register(SERVER_NAME, SERVER_TYPE)

        self.udp_sock = socket(AF_INET, SOCK_DGRAM)
        self.sequence_number = 0
        self.peer_list: list[dict[str, str]] = []
        self.expected_peer_count = 0

    def close(self) -> None:
        try:
            self.naming_client.unbind(SERVER_NAME)
        except Exception:
            pass

        try:
            self.server_sock.close()
        except Exception:
            pass

        try:
            self.udp_sock.close()
        except Exception:
            pass

    def _discover_peers(self) -> list[dict[str, str]]:
        return self.naming_client.discover(PEER_TYPE)

    def get_peer_list(self, wait: bool = True, poll_interval: float = 1.0) -> list[dict[str, str]]:
        while True:
            peers = self._discover_peers()
            if peers or not wait:
                self.peer_list = peers
                self.expected_peer_count = len(peers)
                print("[Server] Discovered peers:", peers)
                return peers

            print("[Server] No peers discovered yet. Waiting for registrations...")
            time.sleep(poll_interval)

    def start_peers(self, peer_list: list[dict[str, str]], n_ops: int) -> None:
        print(f"[Server] Starting {len(peer_list)} peers with {n_ops} operations each...")
        for peer_number, peer in enumerate(sorted(peer_list, key=lambda item: item["nome"])):
            host, port = split_endpoint(peer["endereco"])
            with socket(AF_INET, SOCK_STREAM) as sock:
                sock.connect((host, port))
                msg = pickle.dumps((peer_number, n_ops))
                sock.sendall(msg)
                sock.shutdown(1)
                response = pickle.loads(_read_all(sock))
                print(f"[Server] {peer['nome']}: {response}")

    def _broadcast(self, payload: dict) -> None:
        data = pickle.dumps(payload)
        for peer in self.peer_list:
            host, port = split_endpoint(peer["endereco"])
            self.udp_sock.sendto(data, (host, port))

    def receive_and_sequence_submissions(self, expected_total: int) -> None:
        print(f"[Server] Waiting for {expected_total} submitted operations...")

        received = 0
        while received < expected_total:
            conn, _ = self.server_sock.accept()
            try:
                data = b""
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk

                req = pickle.loads(data)

                if req.get("op") == "submit":
                    self.sequence_number += 1

                    ordered_msg = {
                        "op": "apply",
                        "seq": self.sequence_number,
                        "kind": req["kind"],
                        "from": req["from"],
                        "local_seq": req["local_seq"],
                        "key": req["key"],
                        "value": req.get("value"),
                    }

                    print(
                        f"[Server] seq={self.sequence_number} | "
                        f"peer={req['from']} | local_seq={req['local_seq']} | "
                        f"{req['kind']} {req['key']}"
                        + (f"={req.get('value')}" if req["kind"] == "WRITE" else "")
                    )

                    self._broadcast(ordered_msg)

                    conn.sendall(pickle.dumps({
                        "status": "ok",
                        "seq": self.sequence_number,
                    }))

                    received += 1

                else:
                    conn.sendall(pickle.dumps({"status": "ignored"}))

            finally:
                conn.close()

    def broadcast_end_marker(self) -> None:
        self.sequence_number += 1
        end_msg = {
            "op": "apply",
            "seq": self.sequence_number,
            "kind": "END",
            "from": "server",
            "local_seq": -1,
            "key": None,
            "value": None,
        }
        print(f"[Server] Broadcasting END marker as seq={self.sequence_number}")
        self._broadcast(end_msg)

    def collect_final_states(self, expected_count: int) -> list[dict]:
        states = []
        print(f"[Server] Waiting for final states from {expected_count} peers...")

        while len(states) < expected_count:
            conn, _ = self.server_sock.accept()
            try:
                data = b""
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk

                state = pickle.loads(data)
                if state.get("op") == "final_state":
                    states.append(state)
                    print(
                        f"[Server] Received final state from peer {state['peer']} "
                        f"with {len(state['db'])} records"
                    )
                    conn.sendall(pickle.dumps({"status": "received"}))
                else:
                    conn.sendall(pickle.dumps({"status": "ignored"}))
            finally:
                conn.close()

        return states

    def compare_final_states(self, states: list[dict]) -> None:
        if not states:
            print("[Server] No final states received.")
            return

        reference = states[0]["db"]
        ok = True

        for state in states[1:]:
            if state["db"] != reference:
                ok = False
                print(f"[Server] Replica mismatch detected on peer {state['peer']}")

        if ok:
            print("[Server] All replicas ended with the same database content.")
        else:
            print("[Server] Final replica contents:")
            for state in states:
                print(f"  peer={state['peer']} db={state['db']}")

    @staticmethod
    def prompt_user() -> int:
        return int(input("Enter the number of operations for each peer to submit (0 to terminate)=> "))

    def run(self):
        try:
            while True:
                n_ops = self.prompt_user()
                peer_list = self.get_peer_list(wait=(n_ops != 0))
                self.start_peers(peer_list, n_ops)

                if n_ops != 0:
                    expected_total = len(peer_list) * n_ops
                    print("[Server] Peers started. Sequencing commands now...")

                    self.sequence_number = 0

                    self.receive_and_sequence_submissions(expected_total)
                    self.broadcast_end_marker()

                    states = self.collect_final_states(len(peer_list))
                    self.compare_final_states(states)
                else:
                    print("[Server] Stopping.")
                    break
        finally:
            self.close()


if __name__ == "__main__":
    server = ComparisonServer()
    server.run()