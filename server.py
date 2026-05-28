#Socket server node for DistRes
import json
import socketserver

import database
import conRes


HOST = "127.0.0.1"
PORT = 5050

#Initialises the server-owned database before accepting client requests
database.init()

#Creates one shared DistRes engine for all connected client nodes
distRes = conRes.DistRes(capacity = 4)


class DistResRequestHandler(socketserver.StreamRequestHandler):
    #Handles one connected client socket

    def handle(self):
        #Reads one JSON request per line from the client connection
        for line in self.rfile:
            try:
                request = json.loads(line.decode("utf-8"))
                action = request.get("action")
                payload = request.get("payload", {})

                #Routes the requested action to the server-side DistRes engine
                response = handle_action(action, payload)

            except Exception as error:
                #Returns an error response instead of crashing the server thread
                response = {"okay": False, "error": str(error)}

            #Sends one JSON response line back to the client
            self.wfile.write(json.dumps(response).encode("utf-8") + b"\n")
            self.wfile.flush()


def handle_action(action, payload):
    #Routes socket actions to the DistRes engine

    if action == "state":
        #Returns active sessions, lock status, logs and shared file contents
        return distRes.status()

    if action == "login":
        #Authenticates the user and creates or queues a server session
        return distRes.login(payload["user_id"], payload["password"])

    if action == "logout":
        #Releases the user session and any locks held by that user
        return distRes.logout(payload["user_id"])

    if action == "read":
        #Requests a shared read lock for the selected distributed file
        return distRes.acquire_read_lock(
            payload["user_id"],
            payload.get("resource", "product.txt")
        )

    if action == "write":
        #Requests an exclusive write lock for the selected distributed file
        return distRes.acquire_write_lock(
            payload["user_id"],
            payload.get("resource", "product.txt")
        )

    if action == "release":
        #Releases the selected file lock held by the user
        return distRes.release(
            payload["user_id"],
            payload.get("resource")
        )

    if action == "commit":
        #Saves file content only if this user owns the write lock
        return distRes.commit_write(
            payload["user_id"],
            payload["content"],
            payload.get("resource", "product.txt")
        )

    if action == "users":
        #Returns registered users from the server-owned database
        return {"okay": True, "users": database.list_users()}

    if action == "audit_log":
        #Returns audit log entries recorded by the server
        return {"okay": True, "entries": database.read_audit_log()}

    return {"okay": False, "error": "Unknown action: " + str(action)}


class DistResSocketServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    #Allows multiple clients to connect at the same time

    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    #Starts the socket server and keeps it running until stopped
    with DistResSocketServer((HOST, PORT), DistResRequestHandler) as server:
        print("DistRes server running on " + HOST + ":" + str(PORT))
        server.serve_forever()