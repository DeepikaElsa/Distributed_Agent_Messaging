Assignment 2.2: Distributed Agent Messaging using Sockets
==========================================================

Files
-----
protocol.py       Shared wire protocol (framing, JSON serialization, validation)
agent_b.py        Agent-B, the receiving agent (TCP server)
agent_a.py        Agent-A, the sending agent (TCP client)
execution_log.txt Captured output of one full run of both terminals

How to run (two terminals, same folder, Python 3, no extra packages)
--------------------------------------------------------------------
Terminal 1:   python agent_b.py
Terminal 2:   python agent_a.py

Start Agent-B first. Agent-A retries the connection up to 10 times, so
starting it slightly early is also fine. Both use 127.0.0.1:5050.

Design
------
Architecture    Client-Server over raw TCP sockets (socket module). Agent-B
                listens and spawns one thread per connection. Agent-A connects
                and runs a separate receiver thread so it can react to messages
                Agent-B sends on its own initiative.

Serialization   Every message is a JSON object with the fields
                type, sender, seq, ts, payload.
                On the wire: 4-byte big-endian length prefix + UTF-8 JSON body.
                The length prefix is needed because TCP is a byte stream and
                does not preserve message boundaries. recv_exact() loops until
                the full header and body are read.
                Message types: PING, PONG, STATE_TRANSFER, COMMAND, ACK, ERROR.

State payload  Agent-A transfers its current state (temperature, humidity,
                sampling_interval, alert_threshold), the same kind of
                environmental readings used in the Background_Processing_Agent.

Proactive handling
                Agent-B does not just store the state. If the transferred
                temperature is above the alert threshold, it sends a COOL_DOWN
                COMMAND back to Agent-A without being asked. Agent-A's receiver
                thread picks it up, lowers its temperature, changes its
                sampling interval, and replies with an ACK. Agent-B also
                executes remote instructions in COMMAND payloads
                (SET_PARAM, REPORT_STATUS, SHUTDOWN).

Stability       - SO_REUSEADDR so the port can be reused right after a run
                - connect retry loop on the client
                - per-connection threads and a lock around shared state
                - idle timeout on server connections
                - 1 MB message size limit
                - malformed JSON, unknown types and bad payloads raise
                  ProtocolError; Agent-B replies with an ERROR message and
                  keeps serving
                - unknown commands get an ERROR reply instead of a crash
                - clean shutdown through a SHUTDOWN command

Demo scenario (what execution_log.txt shows)
--------------------------------------------
1. PING / PONG connectivity check
2. Agent-A transfers a normal state (28.5 C), Agent-B ACKs
3. Agent-A transfers an overheated state (47.2 C), Agent-B ACKs and then
   proactively sends COOL_DOWN; Agent-A acts on it (37.2 C, interval 1)
4. Remote parameter change (SET_PARAM)
5. Status query (REPORT_STATUS)
6. Unknown command, handled with an ERROR reply
7. SHUTDOWN, Agent-B exits cleanly

Rubric mapping
--------------
Network Socket/RPC Stability (2)     TCP client/server, retries, threads, locks,
                                     timeouts, error replies, clean shutdown
Data Serialization & Transport (1.5) JSON + length-prefixed framing in protocol.py
Proactive Remote Message Handling (1.5)
                                     Agent-B COOL_DOWN push, Agent-A receiver
                                     thread, command dispatch on both sides
