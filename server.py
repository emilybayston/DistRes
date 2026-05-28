#Socket server for DistRes
#Clients send JSON actions to this server over TCP
#The server routes those actions to the coordination engine and data layer
#It also keeps subscriber sockets open for write update notifications

import json
import select
import socket
import socketserver
import threading
import time
from datetime import datetime

import distRes
import dataLayer


HOST = "127.0.0.1"
PORT = 5050


class FaultToleranceManager:
    #Keeps bad client requests from taking down the server

    #Runs a client action safely and sends back an error instead of crashing
    def safe_handle(self, action, payload, coordinator):
        #Runs one request and turns unexpected errors into JSON replies
        #One broken request should not crash the whole socket server
        try:
            return coordinator.handle_action(action, payload)
        except KeyError as error:
            return {"okay": False, "error": "Missing request field: " + str(error)}
        except Exception as error:
            return {"okay": False, "error": str(error)}

    #Turns one socket line into an action and payload the server can use
    def decode_request(self, line):
        #Reads one JSON message sent by a client socket
        #The action says which server operation the client wants
        request = json.loads(line.decode("utf-8"))
        return request.get("action"), request.get("payload", {})

    #Turns one server response into the JSON line expected by the client
    def encode_response(self, response):
        #Turns a response dictionary back into a JSON line for the client
        return json.dumps(response).encode("utf-8") + b"\n"


class PublishSubscribeService:
    #Keeps track of clients waiting for write update notifications

    #Creates an empty subscriber list shared by all socket threads
    def __init__(self):
        #A lock is needed because several socket threads may subscribe at the same time
        self.subscribers = {}
        self.counter = 1
        self.lock = threading.Lock()

    #Adds a connected client to the update notification list
    def add(self, handler, nodeId):
        #Stores this open socket so commits can push updates to it later
        with self.lock:
            subscriberId = "Subscriber" + str(self.counter)
            self.counter += 1
            self.subscribers[subscriberId] = {
                "handler": handler,
                "nodeId": nodeId,
                "connectedAt": datetime.now().strftime("%H:%M:%S")
            }
            return subscriberId

    #Removes a client when its subscription connection has ended
    def remove_handler(self, handler):
        #Drops a subscriber once its socket has closed or stopped accepting events
        with self.lock:
            removeIds = [
                subscriberId for subscriberId, details in self.subscribers.items()
                if details["handler"] is handler
            ]
            for subscriberId in removeIds:
                del self.subscribers[subscriberId]

    #Pushes one write update to every client still connected as a subscriber
    def publish(self, event):
        #Sends a committed write event to every connected subscriber
        #Any dead subscriber sockets are cleaned up during the publish
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

    #Gives the dashboard a simple view of active subscribers
    def status(self):
        #Returns a small subscriber summary for the dashboard
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
    #Maps incoming client actions to the correct server-side operation

    #Keeps references to the coordination engine and notification service
    def __init__(self, distRes, subscribers):
        #The coordinator does not own resources, it just directs traffic
        self.distRes = distRes
        self.subscribers = subscribers

    #Chooses which server operation should run for the requested action
    def handle_action(self, action, payload):
        #Matches the client's action name to the application code that handles it
        if action == "state":
            #State is used by the dashboard to redraw sessions, locks and files
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
                payload.get("resource", "ProductSpecification.txt")
            )

        if action == "write":
            #Write requests ask for exclusive access before the file can be edited
            return self.distRes.acquire_write_lock(
                payload["user_id"],
                payload.get("resource", "ProductSpecification.txt")
            )

        if action == "release":
            return self.distRes.release(
                payload["user_id"],
                payload.get("resource")
            )

        if action == "commit":
            #Only a successful write commit is published to subscribers
            resource = payload.get("resource", "ProductSpecification.txt")
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

    #Builds the server application layer before clients start sending requests
    def __init__(self, capacity = 4):
        #Initialises storage once before any client requests are accepted
        dataLayer.userCredentialData.init()
        self.distRes = distRes.DistRes(capacity = capacity)
        self.subscribers = PublishSubscribeService()
        self.coordinator = DistributedRequestCoordinator(self.distRes, self.subscribers)
        self.faultTolerance = FaultToleranceManager()

    #Registers a client as a subscriber for future write notifications
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

    #Runs a decoded action through the fault tolerance wrapper
    def process_action(self, action, payload):
        #Runs an already decoded request through the fault tolerant coordinator
        return self.faultTolerance.safe_handle(action, payload, self.coordinator)

    #Processes one raw socket message, mainly useful for simple tests
    def process_line(self, line):
        #Processes one socket request and returns one socket response
        try:
            action, payload = self.faultTolerance.decode_request(line)
            response = self.process_action(action, payload)
        except Exception as error:
            response = {"okay": False, "error": str(error)}
        return self.faultTolerance.encode_response(response)


socketInterface = SocketInterface(capacity = 4)


class DistResRequestHandler(socketserver.StreamRequestHandler):
    #Handles one connected client socket

    #Adds a lock so two replies are not written to the same socket at once
    def setup(self):
        #Creates a write lock so pushed events and direct replies cannot overlap
        super().setup()
        self.writeLock = threading.Lock()

    #Sends one JSON object back to this connected client
    def send_json(self, response):
        #Writes one JSON response or event to this client socket
        with self.writeLock:
            self.wfile.write(socketInterface.faultTolerance.encode_response(response))
            self.wfile.flush()

    #Sends a publish-subscribe event if this client is still connected
    def send_event(self, event):
        #Pushes a publish-subscribe event to a connected subscriber socket
        try:
            self.send_json({"kind": "event", "data": event})
            return True
        except OSError:
            return False

    #Handles both normal request sockets and subscription sockets
    def handle(self):
        #Reads JSON requests until the client disconnects
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
