#Socket server node for DistRes
import json
import select
import socket
import socketserver
import threading
import time
from datetime import datetime

import conRes
import dataLayer


HOST = "127.0.0.1"
PORT = 5050


class FaultToleranceManager:
    #Keeps client request failures isolated from the main server

    def safe_handle(self, action, payload, coordinator):
        #Runs one request and converts failures into safe JSON responses
        #This stops one failed client request from crashing the server thread
        try:
            return coordinator.handle_action(action, payload)
        except KeyError as error:
            return {"okay": False, "error": "Missing request field: " + str(error)}
        except Exception as error:
            return {"okay": False, "error": str(error)}

    def decode_request(self, line):
        #Decodes one JSON request line from a client socket
        #Each request works like a small RPC call with an action and payload
        request = json.loads(line.decode("utf-8"))
        return request.get("action"), request.get("payload", {})

    def encode_response(self, response):
        #Encodes one JSON response line for the client socket
        return json.dumps(response).encode("utf-8") + b"\n"


class PublishSubscribeService:
    #Tracks clients waiting for server-side resource update notifications

    def __init__(self):
        #The dictionary is protected because several socket threads can subscribe at once
        self.subscribers = {}
        self.counter = 1
        self.lock = threading.Lock()

    def add(self, handler, nodeId):
        #Stores the open socket handler so later commits can push updates to it
        with self.lock:
            subscriberId = "Subscriber" + str(self.counter)
            self.counter += 1
            self.subscribers[subscriberId] = {
                "handler": handler,
                "nodeId": nodeId,
                "connectedAt": datetime.now().strftime("%H:%M:%S")
            }
            return subscriberId

    def remove_handler(self, handler):
        #Removes a subscriber when its socket closes or stops accepting events
        with self.lock:
            removeIds = [
                subscriberId for subscriberId, details in self.subscribers.items()
                if details["handler"] is handler
            ]
            for subscriberId in removeIds:
                del self.subscribers[subscriberId]

    def publish(self, event):
        #Sends one committed update event to every connected subscriber
        #Stale clients are removed so old browser sessions do not keep receiving events
        staleHandlers = []
        notified = 0
        with self.lock:
            subscribers = list(self.subscribers.values())

        for details in subscribers:
            if details["handler"].send_event(event):
                notified += 1
            else:
                staleHandlers.append(details["handler"])

        for handler in staleHandlers:
            self.remove_handler(handler)

        return notified

    def status(self):
        #Returns a small dashboard snapshot of current subscription sockets
        with self.lock:
            return {
                "count": len(self.subscribers),
                "nodes": [
                    {
                        "subscriberId": subscriberId,
                        "nodeId": details["nodeId"],
                        "connectedAt": details["connectedAt"]
                    }
                    for subscriberId, details in self.subscribers.items()
                ]
            }


class DistributedRequestCoordinator:
    #Routes client actions to the server-side DistRes engine and data layer

    def __init__(self, distRes, subscribers):
        #The coordinator owns no resources itself, it only directs requests to the right service
        self.distRes = distRes
        self.subscribers = subscribers

    def handle_action(self, action, payload):
        #Matches socket action names to application-layer operations
        if action == "state":
            #State combines coordination data and subscriber data for dashboard polling
            state = self.distRes.status()
            state["subscribers"] = self.subscribers.status()
            return state

        if action == "login":
            return self.distRes.login(payload["user_id"], payload["password"])

        if action == "logout":
            return self.distRes.logout(payload["user_id"])

        if action == "read":
            #Read requests are passed to the application layer so lock rules are enforced first
            return self.distRes.acquire_read_lock(
                payload["user_id"],
                payload.get("resource", "product.txt")
            )

        if action == "write":
            #Write requests ask for exclusive access before the file can be edited
            return self.distRes.acquire_write_lock(
                payload["user_id"],
                payload.get("resource", "product.txt")
            )

        if action == "release":
            return self.distRes.release(
                payload["user_id"],
                payload.get("resource")
            )

        if action == "commit":
            #Only a successful write commit is published to subscribers
            resource = payload.get("resource", "product.txt")
            result = self.distRes.commit_write(
                payload["user_id"],
                payload["content"],
                resource
            )
            if result.get("okay"):
                event = {
                    "type": "resource_updated",
                    "resource": result.get("resource", resource),
                    "user_id": payload["user_id"],
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "message": result.get("resource", resource) + " updated by " + payload["user_id"]
                }
                result["notified"] = self.subscribers.publish(event)
            return result

        if action == "users":
            #User queries go through the data layer, but are triggered by server-side actions
            return {"okay": True, "users": dataLayer.userCredentialData.list_users()}

        if action == "register_user":
            okay, error = dataLayer.userCredentialData.register_user(
                payload["user_id"],
                payload["username"],
                payload.get("role", "Engineer"),
                payload["password"]
            )
            if okay:
                self.distRes.log.add(
                    "Registered: " + payload["user_id"] + " (" + payload["username"] + ")",
                    "LOGIN"
                )
            return {"okay": okay, "error": error}

        if action == "delete_user":
            userId = payload["user_id"]
            if (self.distRes.activeUsers.contains(userId)):
                return {"okay": False, "error": userId + " is logged in. Logout first."}
            dataLayer.userCredentialData.delete_user(userId)
            self.distRes.log.add("Deleted: " + userId, "WARN")
            return {"okay": True}

        if action == "change_password":
            if (not dataLayer.userCredentialData.authenticate(payload["user_id"], payload["current_password"])):
                return {"okay": False, "error": "Current password incorrect."}
            dataLayer.userCredentialData.change_password(payload["user_id"], payload["new_password"])
            self.distRes.log.add("Password Changed: " + payload["user_id"], "INFO")
            return {"okay": True}

        if action == "audit_log":
            return {"okay": True, "entries": dataLayer.userCredentialData.read_audit_log()}

        if action == "reconfigure":
            self.distRes.update_slots(int(payload["capacity"]))
            return {"okay": True}

        if action == "clear_log":
            self.distRes.log.clear()
            return {"okay": True}

        return {"okay": False, "error": "Unknown action: " + str(action)}


class SocketInterface:
    #Owns the DistRes engine and the server-side request helpers

    def __init__(self, capacity = 4):
        #Initialises storage once before any client requests are accepted
        dataLayer.userCredentialData.init()
        self.distRes = conRes.DistRes(capacity = capacity)
        self.subscribers = PublishSubscribeService()
        self.coordinator = DistributedRequestCoordinator(self.distRes, self.subscribers)
        self.faultTolerance = FaultToleranceManager()

    def subscribe(self, handler, payload):
        #Registers a long-lived client socket for publish-subscribe events
        #Unlike normal requests, this socket stays open so updates can be pushed later
        nodeId = payload.get("node_id", "browser-client")
        subscriberId = self.subscribers.add(handler, nodeId)
        return {
            "okay": True,
            "subscriberId": subscriberId,
            "message": "Subscribed to server write updates"
        }

    def process_action(self, action, payload):
        #Runs an already decoded request through the fault tolerant coordinator
        return self.faultTolerance.safe_handle(action, payload, self.coordinator)

    def process_line(self, line):
        #Processes one socket request and returns one socket response
        try:
            action, payload = self.faultTolerance.decode_request(line)
            response = self.process_action(action, payload)
        except Exception as error:
            response = {"okay": False, "error": str(error)}
        return self.faultTolerance.encode_response(response)


SocketRPCInterface = SocketInterface
socketInterface = SocketInterface(capacity = 4)


class DistResRequestHandler(socketserver.StreamRequestHandler):
    #Handles one connected client socket

    def setup(self):
        #Creates a write lock so pushed events and direct replies cannot overlap
        super().setup()
        self.writeLock = threading.Lock()

    def send_json(self, response):
        #Writes one JSON response or event to this client socket
        with self.writeLock:
            self.wfile.write(socketInterface.faultTolerance.encode_response(response))
            self.wfile.flush()

    def send_event(self, event):
        #Pushes a publish-subscribe event to a connected subscriber socket
        try:
            self.send_json({"kind": "event", "data": event})
            return True
        except OSError:
            return False

    def handle(self):
        #Reads newline-delimited JSON requests until the client disconnects
        try:
            for line in self.rfile:
                action, payload = socketInterface.faultTolerance.decode_request(line)
                if action == "subscribe":
                    #Subscription requests are kept open instead of closing after one reply
                    response = socketInterface.subscribe(self, payload)
                    self.send_json({"kind": "subscribed", "data": response})
                    while True:
                        #Peeks at the socket so the server can notice closed browser clients
                        readable, _, _ = select.select([self.request], [], [], 1)
                        if readable:
                            data = self.request.recv(1, socket.MSG_PEEK)
                            if not data:
                                break
                        time.sleep(0.1)

                response = socketInterface.process_action(action, payload)
                self.send_json(response)
        finally:
            socketInterface.subscribers.remove_handler(self)


class DistResSocketServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    #Allows multiple clients to connect at the same time

    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    #Starts the socket server and keeps it running until stopped
    with DistResSocketServer((HOST, PORT), DistResRequestHandler) as server:
        print("DistRes server running on " + HOST + ":" + str(PORT))
        server.serve_forever()
