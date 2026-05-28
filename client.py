#Socket client wrapper for DistRes
import json
import socket
import threading
import time


class DistResClient:
    #Handles communication from the client side to the DistRes server

    def __init__(self, host = "127.0.0.1", port = 5050, retries = 3, retryDelay = 5, recoveryDelay = 15, showRetries = False):
        #These settings keep the client pointed at the local DistRes server node
        self.host = host
        self.port = port
        self.retries = retries
        #Retry timing follows the taught fault tolerance pattern of retrying before waiting longer
        self.retryDelay = retryDelay
        self.recoveryDelay = recoveryDelay
        self.showRetries = showRetries
        #Received publish-subscribe events are stored here until Flask sends them to the UI
        self.events = []
        self.eventsLock = threading.Lock()
        self.subscriptionThread = None
        self.subscriptionRunning = False

    def record_event(self, event):
        #Keeps recent server pushed notifications for the browser dashboard
        with self.eventsLock:
            #Newest events are shown first so the dashboard reflects the latest write update
            self.events.insert(0, event)
            self.events = self.events[:100]

    def recent_events(self):
        #Returns a copy so Flask can read notifications without changing them
        with self.eventsLock:
            return list(self.events)

    def start_subscription(self, nodeId = "browser-client"):
        #Starts one background listener for publish-subscribe server updates
        #The guard prevents multiple listener threads from one Flask process
        if (self.subscriptionThread and self.subscriptionThread.is_alive()):
            return

        self.subscriptionRunning = True
        self.subscriptionThread = threading.Thread(
            target = self.subscription_loop,
            args = (nodeId,),
            daemon = True
        )
        self.subscriptionThread.start()

    def subscription_loop(self, nodeId):
        #Keeps reconnecting so committed writes can be pushed to the UI
        while self.subscriptionRunning:
            try:
                with socket.create_connection((self.host, self.port), timeout = 3) as sock:
                    #Connect quickly, then wait normally for pushed server events
                    sock.settimeout(None)
                    file = sock.makefile("rwb")
                    message = {
                        "action": "subscribe",
                        "payload": {"node_id": nodeId}
                    }
                    file.write(json.dumps(message).encode("utf-8") + b"\n")
                    file.flush()

                    for line in file:
                        #The server sends subscription acknowledgements and later write events on this stream
                        response = json.loads(line.decode("utf-8"))
                        if response.get("kind") == "event":
                            self.record_event(response.get("data", {}))
                        elif response.get("kind") == "subscribed":
                            data = response.get("data", {})
                            self.record_event({
                                "type": "subscription",
                                "resource": "server",
                                "user_id": nodeId,
                                "time": time.strftime("%H:%M:%S"),
                                "message": data.get("message", "Subscribed to server write updates")
                            })

            except (OSError, json.JSONDecodeError) as error:
                #A broken subscription is recorded for visibility, then retried after the recovery delay
                self.record_event({
                    "type": "subscription_error",
                    "resource": "server",
                    "user_id": nodeId,
                    "time": time.strftime("%H:%M:%S"),
                    "message": "Subscription disconnected: " + str(error)
                })
                time.sleep(self.recoveryDelay)

    def request(self, action, payload = None):
        #Builds a request message and sends it to the server

        payload = payload or {}
        #The action name tells the server which application-layer operation to run
        message = {
            "action": action,
            "payload": payload
        }

        lastError = "Server unavailable"

        for attempt in range(1, self.retries + 1):
            try:
                if (self.showRetries):
                    print("DistRes client attempt " + str(attempt) + "/" + str(self.retries))

                #Open a new connection for this request
                with socket.create_connection((self.host, self.port), timeout = 3) as sock:
                    file = sock.makefile("rwb")

                    #Send the request as a JSON line
                    file.write(json.dumps(message).encode("utf-8") + b"\n")
                    file.flush()

                    #Wait for the server response
                    response = file.readline()
                    if not response:
                        return {"okay": False, "error": "No response from server"}

                    #The response is decoded back into a dictionary for Flask routes to return
                    result = json.loads(response.decode("utf-8"))
                    if isinstance(result, dict):
                        result["attempt"] = attempt
                    return result

            except (OSError, json.JSONDecodeError) as error:
                #Retries three times before pausing for a longer recovery wait
                lastError = str(error)
                if (self.showRetries):
                    print("DistRes retry " + str(attempt) + " failed: " + lastError)
                if (attempt < self.retries):
                    time.sleep(self.retryDelay)
                else:
                    time.sleep(self.recoveryDelay)

        return {
            "okay": False,
            "error": "Could not connect to DistRes server after " + str(self.retries) + " attempts: " + lastError,
            "attempts": self.retries,
            "retryDelay": self.retryDelay,
            "recoveryDelay": self.recoveryDelay
        }
