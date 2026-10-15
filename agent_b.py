"""
agent_b.py - Agent-B: the receiving agent (TCP server).

Run this first, in Terminal 1:   python agent_b.py

Agent-B accepts connections, deserializes each message and reacts
autonomously based on the instructions inside the payload:
  PING            -> replies PONG
  STATE_TRANSFER  -> adopts Agent-A's parameters, replies ACK, and if the
                     transferred temperature exceeds its alert threshold it
                     PROACTIVELY sends a COMMAND back (COOL_DOWN) without
                     being asked
  COMMAND         -> executes SET_PARAM / REPORT_STATUS / SHUTDOWN
  anything bad    -> replies ERROR and keeps running
"""
import socket
import threading

from protocol import (HOST, PORT, ProtocolError, make_msg, recv_msg,
                      send_msg)

NAME = "Agent-B"


class AgentB:
    def __init__(self):
        self.lock = threading.Lock()
        self.params = {"sampling_interval": 5, "alert_threshold": 40.0}
        self.last_state = None
        self.seq = 0
        self.stop = threading.Event()
        self.server = None

    def log(self, text):
        print(f"[{NAME}] {text}", flush=True)

    def next_seq(self):
        with self.lock:
            self.seq += 1
            return self.seq

    # ---------------- message handlers ----------------
    def on_ping(self, conn, msg):
        send_msg(conn, make_msg("PONG", NAME, {"status": "alive"}, msg["seq"]))

    def on_state(self, conn, msg):
        state = msg["payload"]
        with self.lock:
            self.last_state = state
            for key in ("sampling_interval", "alert_threshold"):
                if key in state:
                    self.params[key] = state[key]
            threshold = self.params["alert_threshold"]
        self.log(f"state received from {msg['sender']}: {state}")
        send_msg(conn, make_msg("ACK", NAME,
                                {"info": "state adopted"}, msg["seq"]))

        # Proactive behaviour: act on the payload without being asked.
        temp = state.get("temperature")
        if temp is not None and temp > threshold:
            self.log(f"temperature {temp} > threshold {threshold}: "
                     f"sending proactive COOL_DOWN command")
            cmd = {"action": "COOL_DOWN",
                   "reason": f"temperature {temp} above {threshold}",
                   "new_sampling_interval": 1}
            send_msg(conn, make_msg("COMMAND", NAME, cmd, self.next_seq()))

    def on_command(self, conn, msg):
        p = msg["payload"]
        action = p.get("action")
        self.log(f"command received: {p}")
        if action == "SET_PARAM":
            key, value = p.get("key"), p.get("value")
            with self.lock:
                self.params[key] = value
            send_msg(conn, make_msg("ACK", NAME,
                                    {"info": f"{key} set to {value}"},
                                    msg["seq"]))
        elif action == "REPORT_STATUS":
            with self.lock:
                report = {"params": dict(self.params),
                          "last_state": self.last_state}
            send_msg(conn, make_msg("ACK", NAME, report, msg["seq"]))
        elif action == "SHUTDOWN":
            send_msg(conn, make_msg("ACK", NAME,
                                    {"info": "shutting down"}, msg["seq"]))
            self.log("shutdown requested by peer")
            self.stop.set()
            try:  # unblock accept()
                socket.create_connection((HOST, PORT), timeout=1).close()
            except OSError:
                pass
        else:
            send_msg(conn, make_msg("ERROR", NAME,
                                    {"error": f"unknown action {action}"},
                                    msg["seq"]))

    # ---------------- connection loop ----------------
    def handle_client(self, conn, addr):
        self.log(f"connection from {addr}")
        conn.settimeout(60)
        try:
            while not self.stop.is_set():
                try:
                    msg = recv_msg(conn)
                except ProtocolError as exc:
                    self.log(f"protocol error: {exc}")
                    send_msg(conn, make_msg("ERROR", NAME, {"error": str(exc)}))
                    break
                except socket.timeout:
                    self.log("client idle, closing")
                    break
                if msg is None:
                    break
                handler = {"PING": self.on_ping,
                           "STATE_TRANSFER": self.on_state,
                           "COMMAND": self.on_command}.get(msg["type"])
                if handler:
                    handler(conn, msg)
                # ACK / PONG / ERROR from the peer need no reply
        except (ConnectionError, OSError) as exc:
            self.log(f"connection problem: {exc}")
        finally:
            conn.close()
            self.log(f"connection from {addr} closed")

    def serve(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((HOST, PORT))
        self.server.listen(5)
        self.log(f"listening on {HOST}:{PORT}")
        try:
            while not self.stop.is_set():
                conn, addr = self.server.accept()
                if self.stop.is_set():
                    conn.close()
                    break
                threading.Thread(target=self.handle_client,
                                 args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            self.log("interrupted")
        finally:
            self.server.close()
            self.log("server stopped")


if __name__ == "__main__":
    AgentB().serve()
