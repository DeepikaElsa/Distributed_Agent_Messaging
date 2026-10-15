"""
agent_a.py - Agent-A: the sending agent (TCP client).

Run this second, in Terminal 2:   python agent_a.py

Agent-A holds its active state (the same kind of environmental readings as
the Background_Processing_Agent), serializes it to JSON and transfers it to
Agent-B over a local socket. A receiver thread listens for proactive
messages from Agent-B and reacts autonomously (e.g. COOL_DOWN).
"""
import queue
import socket
import threading
import time

from protocol import (HOST, PORT, ProtocolError, make_msg, recv_msg,
                      send_msg)

NAME = "Agent-A"


class AgentA:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = {
            "agent_id": NAME,
            "temperature": 28.5,
            "humidity": 61.0,
            "sampling_interval": 5,
            "alert_threshold": 40.0,
        }
        self.seq = 0
        self.sock = None
        self.acks = queue.Queue()        # replies matched to our requests
        self.closed = threading.Event()

    def log(self, text):
        print(f"[{NAME}] {text}", flush=True)

    def next_seq(self):
        with self.lock:
            self.seq += 1
            return self.seq

    # ---------------- connection ----------------
    def connect(self, retries=10, delay=1.0):
        for attempt in range(1, retries + 1):
            try:
                self.sock = socket.create_connection((HOST, PORT), timeout=5)
                self.sock.settimeout(None)
                self.log(f"connected to Agent-B at {HOST}:{PORT}")
                threading.Thread(target=self.receiver, daemon=True).start()
                return
            except OSError:
                self.log(f"Agent-B not ready (attempt {attempt}/{retries}), "
                         f"retrying in {delay}s")
                time.sleep(delay)
        raise SystemExit("could not reach Agent-B; start agent_b.py first")

    # ---------------- receiving (proactive messages) ----------------
    def receiver(self):
        try:
            while True:
                msg = recv_msg(self.sock)
                if msg is None:
                    break
                if msg["type"] == "COMMAND":
                    self.handle_command(msg)
                else:
                    self.acks.put(msg)
        except (ProtocolError, OSError) as exc:
            if not self.closed.is_set():
                self.log(f"receiver error: {exc}")
        finally:
            self.closed.set()
            self.acks.put(None)

    def handle_command(self, msg):
        """Autonomous reaction to an unsolicited command from Agent-B."""
        p = msg["payload"]
        self.log(f"PROACTIVE message from {msg['sender']}: {p}")
        if p.get("action") == "COOL_DOWN":
            with self.lock:
                self.state["temperature"] = round(
                    self.state["temperature"] - 10, 1)
                self.state["sampling_interval"] = p.get(
                    "new_sampling_interval", self.state["sampling_interval"])
                snapshot = dict(self.state)
            self.log(f"acting on COOL_DOWN, new state: {snapshot}")
            send_msg(self.sock, make_msg(
                "ACK", NAME, {"info": "cooling applied", "state": snapshot},
                msg["seq"]))
        else:
            send_msg(self.sock, make_msg(
                "ERROR", NAME, {"error": "unsupported action"}, msg["seq"]))

    # ---------------- sending ----------------
    def request(self, msg_type, payload, timeout=5):
        seq = self.next_seq()
        self.log(f"-> {msg_type}: {payload}")
        send_msg(self.sock, make_msg(msg_type, NAME, payload, seq))
        try:
            reply = self.acks.get(timeout=timeout)
        except queue.Empty:
            self.log(f"no reply to seq {seq} within {timeout}s")
            return None
        if reply is not None:
            self.log(f"<- {reply['type']}: {reply['payload']}")
        return reply

    def transfer_state(self):
        with self.lock:
            snapshot = dict(self.state)
        return self.request("STATE_TRANSFER", snapshot)

    # ---------------- demo scenario ----------------
    def run_demo(self):
        self.connect()
        print("\n--- 1. connectivity check ---")
        self.request("PING", {})

        print("\n--- 2. transfer normal state ---")
        self.transfer_state()

        print("\n--- 3. transfer overheated state (Agent-B reacts proactively) ---")
        with self.lock:
            self.state["temperature"] = 47.2
        self.transfer_state()
        time.sleep(1)  # let the proactive COOL_DOWN exchange finish

        print("\n--- 4. remote parameter change ---")
        self.request("COMMAND", {"action": "SET_PARAM",
                                 "key": "sampling_interval", "value": 2})

        print("\n--- 5. ask Agent-B for its status ---")
        self.request("COMMAND", {"action": "REPORT_STATUS"})

        print("\n--- 6. unknown command (error handling) ---")
        self.request("COMMAND", {"action": "SELF_DESTRUCT"})

        print("\n--- 7. shutdown Agent-B ---")
        self.request("COMMAND", {"action": "SHUTDOWN"})

        self.closed.set()
        self.sock.close()
        self.log("done")


if __name__ == "__main__":
    AgentA().run_demo()
