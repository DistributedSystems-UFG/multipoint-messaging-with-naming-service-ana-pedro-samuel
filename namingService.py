from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
import pickle
from typing import Any, Dict, List, Optional, Tuple

from constMP import SERVICE_NAMES_ADDR, SERVICE_NAMES_TCP_PORT


def _read_all(sock) -> bytes:
    chunks = []
    while True:
        data = sock.recv(4096)
        if not data:
            break
        chunks.append(data)
    return b"".join(chunks)


def _send_pickle(conn, payload: Any) -> None:
    conn.sendall(pickle.dumps(payload))


def _recv_pickle(conn) -> Any:
    raw = _read_all(conn)
    if not raw:
        return None
    return pickle.loads(raw)


def split_endpoint(endpoint: str) -> Tuple[str, int]:
    if ":" not in endpoint:
        raise ValueError("Invalid endpoint format: %r" % endpoint)

    host, port_text = endpoint.rsplit(":", 1)
    if not host:
        raise ValueError("Invalid endpoint host: %r" % endpoint)

    try:
        port = int(port_text)
    except ValueError:
        raise ValueError("Invalid endpoint port: %r" % endpoint)

    if port <= 0 or port > 65535:
        raise ValueError("Invalid endpoint port: %r" % endpoint)

    return host, port


def compose_endpoint(host: str, port: int) -> str:
    return "%s:%d" % (host, port)


def detect_local_ip(remote_host: str = SERVICE_NAMES_ADDR, remote_port: int = SERVICE_NAMES_TCP_PORT) -> str:
    with socket(AF_INET, SOCK_DGRAM) as sock:
        try:
            sock.connect((remote_host, remote_port))
            return sock.getsockname()[0]
        except OSError:
            return "127.0.0.1"


class NamingRecord(object):
    def __init__(self, name: str, address: str, service_type: Optional[str] = None):
        self.name = name
        self.address = address
        self.service_type = service_type


class NamingServiceClient(object):
    def __init__(self, host: str = SERVICE_NAMES_ADDR, port: int = SERVICE_NAMES_TCP_PORT):
        self.host = host
        self.port = port

    def _request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            sock.sendall(pickle.dumps(payload))
            sock.shutdown(1)
            response_raw = _read_all(sock)

        if not response_raw:
            raise RuntimeError("Naming Service returned no response.")

        response = pickle.loads(response_raw)
        if not isinstance(response, dict):
            raise RuntimeError("Naming Service returned an invalid response.")

        return response

    @staticmethod
    def _ensure_ok(response: Dict[str, Any]) -> Dict[str, Any]:
        if response.get("status") != "ok":
            message = response.get("mensagem", "Unknown naming service error.")
            raise RuntimeError(message)
        return response

    def bind(self, name: str, address: str) -> None:
        response = self._request({"op": "bind", "nome": name, "endereco": address})
        self._ensure_ok(response)

    def lookup(self, name: str) -> str:
        response = self._request({"op": "lookup", "nome": name})
        response = self._ensure_ok(response)
        address = response.get("endereco")
        if not isinstance(address, str):
            raise RuntimeError("Naming Service returned an invalid address.")
        return address

    def unbind(self, name: str) -> None:
        response = self._request({"op": "unbind", "nome": name})
        self._ensure_ok(response)

    def register(self, name: str, service_type: str) -> None:
        response = self._request({"op": "register", "nome": name, "tipo": service_type})
        self._ensure_ok(response)

    def discover(self, service_type: str) -> List[Dict[str, str]]:
        response = self._request({"op": "discover", "tipo": service_type})
        response = self._ensure_ok(response)

        records = response.get("registros", [])
        if not isinstance(records, list):
            raise RuntimeError("Naming Service returned an invalid discovery list.")

        normalized = []
        for record in records:
            if not isinstance(record, dict):
                continue
            name = record.get("nome")
            address = record.get("endereco")
            tipo = record.get("tipo")
            if isinstance(name, str) and isinstance(address, str) and isinstance(tipo, str):
                normalized.append({"nome": name, "endereco": address, "tipo": tipo})
        return normalized


class NamingServiceServer(object):
    def __init__(self, bind_host: str = "0.0.0.0", port: int = SERVICE_NAMES_TCP_PORT):
        self.bind_host = bind_host
        self.port = port
        self.records = {}  # type: Dict[str, NamingRecord]

        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind((self.bind_host, self.port))
        self.server_sock.listen(32)

    def _handle_bind(self, req: Dict[str, Any]) -> Dict[str, Any]:
        name = req.get("nome")
        address = req.get("endereco")

        if not isinstance(name, str) or not name.strip():
            return {"status": "erro", "mensagem": "Nome inválido."}
        if not isinstance(address, str) or not address.strip():
            return {"status": "erro", "mensagem": "Endereço inválido."}
        if name in self.records:
            return {"status": "erro", "mensagem": "O nome %r já está registrado." % name}

        self.records[name] = NamingRecord(name=name, address=address)
        return {"status": "ok"}

    def _handle_lookup(self, req: Dict[str, Any]) -> Dict[str, Any]:
        name = req.get("nome")
        if not isinstance(name, str) or not name.strip():
            return {"status": "erro", "mensagem": "Nome inválido."}

        record = self.records.get(name)
        if record is None:
            return {"status": "erro", "mensagem": "O nome %r não existe." % name}

        return {"status": "ok", "endereco": record.address}

    def _handle_unbind(self, req: Dict[str, Any]) -> Dict[str, Any]:
        name = req.get("nome")
        if not isinstance(name, str) or not name.strip():
            return {"status": "erro", "mensagem": "Nome inválido."}

        if name not in self.records:
            return {"status": "erro", "mensagem": "O nome %r não existe." % name}

        del self.records[name]
        return {"status": "ok"}

    def _handle_register(self, req: Dict[str, Any]) -> Dict[str, Any]:
        name = req.get("nome")
        service_type = req.get("tipo")

        if not isinstance(name, str) or not name.strip():
            return {"status": "erro", "mensagem": "Nome inválido."}
        if not isinstance(service_type, str) or not service_type.strip():
            return {"status": "erro", "mensagem": "Tipo inválido."}

        record = self.records.get(name)
        if record is None:
            return {"status": "erro", "mensagem": "O nome %r não existe." % name}

        record.service_type = service_type
        return {"status": "ok"}

    def _handle_discover(self, req: Dict[str, Any]) -> Dict[str, Any]:
        service_type = req.get("tipo")
        if not isinstance(service_type, str) or not service_type.strip():
            return {"status": "erro", "mensagem": "Tipo inválido."}

        records = []
        for record in sorted(self.records.values(), key=lambda item: item.name):
            if record.service_type == service_type:
                records.append(
                    {
                        "nome": record.name,
                        "endereco": record.address,
                        "tipo": record.service_type or "",
                    }
                )

        return {"status": "ok", "registros": records}

    def _dispatch(self, req: Dict[str, Any]) -> Dict[str, Any]:
        op = req.get("op")
        if op == "bind":
            return self._handle_bind(req)
        if op == "lookup":
            return self._handle_lookup(req)
        if op == "unbind":
            return self._handle_unbind(req)
        if op == "register":
            return self._handle_register(req)
        if op == "discover":
            return self._handle_discover(req)
        return {"status": "erro", "mensagem": "Operação desconhecida: %r" % op}

    def _handle_connection(self, conn) -> None:
        try:
            req = _recv_pickle(conn)
            if not isinstance(req, dict):
                _send_pickle(conn, {"status": "erro", "mensagem": "Requisição inválida."})
                return

            response = self._dispatch(req)
            _send_pickle(conn, response)
        except Exception as exc:
            try:
                _send_pickle(conn, {"status": "erro", "mensagem": str(exc)})
            except Exception:
                pass

    def run(self) -> None:
        print("[NamingService] Listening on %s:%d" % (self.bind_host, self.port))
        try:
            while True:
                conn, _ = self.server_sock.accept()
                with conn:
                    self._handle_connection(conn)
        except KeyboardInterrupt:
            print("\n[NamingService] Stopping.")
        finally:
            self.server_sock.close()


def main() -> None:
    server = NamingServiceServer()
    server.run()


if __name__ == "__main__":
    main()
