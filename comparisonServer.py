from socket import *
import pickle
from typing import Optional
from constMP import *


class ComparisonServer:
    def __init__(self):
        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', SERVER_PORT))
        self.server_sock.listen(6)

        self.udp_sock = socket(AF_INET, SOCK_DGRAM)
        self.sequence_number = 0
        self.peer_list: list[str] = []

    def _send_to_group_manager(self, request: dict) -> Optional[bytes]:
        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
            sock.sendall(pickle.dumps(request))
            if request["op"] == "list":
                return sock.recv(2048)
        return None

    def get_peer_list(self) -> list:
        raw = self._send_to_group_manager({"op": "list"})
        peer_list = pickle.loads(raw) if raw else []
        self.peer_list = peer_list
        print("[Server] List of Peers:", peer_list)
        return peer_list

    def stop_group_manager(self):
        self._send_to_group_manager({"op": "stop"})

    def start_peers(self, peer_list: list, n_ops: int):
        print(f"[Server] Starting {len(peer_list)} peers with {n_ops} operations each...")
        for peer_number, peer in enumerate(peer_list):
            with socket(AF_INET, SOCK_STREAM) as sock:
                sock.connect((peer, PEER_TCP_PORT))
                msg = pickle.dumps((peer_number, n_ops))
                sock.sendall(msg)
                response = pickle.loads(sock.recv(512))
                print(f"[Server] {response}")

    def _broadcast(self, payload: dict):
        data = pickle.dumps(payload)
        for peer in self.peer_list:
            self.udp_sock.sendto(data, (peer, PEER_UDP_PORT))
            
    def receive_and_sequence_submissions(self, expected_total: int):
        print(f"[Server] Waiting for {expected_total} submitted operations...")
    
        received = 0
        self.final_states = {}
    
        while received < expected_total or len(self.final_states) < N:
            conn, addr = self.server_sock.accept()
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
                        "seq": self.sequence_number
                    }))
    
                    received += 1
    
                elif req.get("op") == "final_state":
                    peer_id = req["peer"]
                    self.final_states[peer_id] = req
    
                    print(
                        f"[Server] Received final state from peer {peer_id} "
                        f"with {len(req['db'])} records"
                    )
    
                    conn.sendall(pickle.dumps({
                        "status": "received"
                    }))

                else:
                    conn.sendall(pickle.dumps({
                        "status": "ignored"
                    }))
    
            finally:
                conn.close()
            
    def broadcast_end_marker(self):
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

    def collect_final_states(self, expected_count: int) -> list:
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
                states.append(state)
                print(
                    f"[Server] Received final state from peer {state['peer']} "
                    f"with {len(state['db'])} records"
                )
                conn.sendall(pickle.dumps({"status": "received"}))
            finally:
                conn.close()

        return states

    def compare_final_states(self, states: list):
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
        while True:
            n_ops = self.prompt_user()
            peer_list = self.get_peer_list()
            self.start_peers(peer_list, n_ops)

            if n_ops != 0:
                expected_total = len(peer_list) * n_ops
                print("[Server] Peers started. Sequencing commands now...")

                self.sequence_number = 0

                self.receive_and_sequence_submissions(expected_total)
                self.broadcast_end_marker()

                states = list(self.final_states.values())
                self.compare_final_states(states)
            else:
                print("[Server] Stopping.")
                self.server_sock.close()
                self.stop_group_manager()
                break


if __name__ == "__main__":
    server = ComparisonServer()
    server.run()
