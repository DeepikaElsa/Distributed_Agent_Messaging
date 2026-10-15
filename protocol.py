"""
protocol.py - shared wire protocol for Agent-A and Agent-B.

Framing   : 4-byte big-endian length prefix + UTF-8 JSON body.
            TCP is a byte stream, so the prefix lets the receiver know
            exactly where one message ends and the next begins.
Message   : {"type": str, "sender": str, "seq": int, "ts": float, "payload": dict}
Types     : PING, PONG, STATE_TRANSFER, COMMAND, ACK, ERROR
"""
import json
import struct
import time

HOST = "127.0.0.1"
PORT = 5050

HEADER = struct.Struct("!I")
MAX_MESSAGE_BYTES = 1 << 20  # 1 MB safety limit

VALID_TYPES = {"PING", "PONG", "STATE_TRANSFER", "COMMAND", "ACK", "ERROR"}


class ProtocolError(Exception):
    """Raised when a malformed or oversized message is received."""


def make_msg(msg_type, sender, payload=None, seq=0):
    return {
        "type": msg_type,
        "sender": sender,
        "seq": seq,
        "ts": time.time(),
        "payload": payload or {},
    }


def _recv_exact(sock, n):
    """Read exactly n bytes, or return None if the peer closed cleanly."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if not buf:
                return None
            raise ProtocolError("connection closed mid-message")
        buf += chunk
    return buf


def send_msg(sock, msg):
    body = json.dumps(msg).encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message too large")
    sock.sendall(HEADER.pack(len(body)) + body)


def recv_msg(sock):
    """Return one decoded message dict, or None when the peer has closed."""
    header = _recv_exact(sock, HEADER.size)
    if header is None:
        return None
    (length,) = HEADER.unpack(header)
    if length > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"declared length {length} exceeds limit")
    body = _recv_exact(sock, length)
    if body is None:
        raise ProtocolError("connection closed before body")
    try:
        msg = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"invalid JSON: {exc}")
    if not isinstance(msg, dict) or msg.get("type") not in VALID_TYPES:
        raise ProtocolError("unknown or missing message type")
    if not isinstance(msg.get("payload", {}), dict):
        raise ProtocolError("payload must be an object")
    return msg
