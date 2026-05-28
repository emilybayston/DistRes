#Socket client used by api.py to talk to the DistRes server
#Normal requests open a socket, send JSON, then wait for one reply
#The action and payload fields make each message behave like a RPC call
#This file also handles retries and the update subscription

import json
import socket
import threading
import time


class DistResClient:
    #Sets up the server address, retry timing and notification storage
    def __init__(self, host = "127.0.0.1", port = 5050, retries = 3, retryDelay = 5, recoveryDelay = 15, showRetries = False):
        #Stores where the server is and how patient this client should be
        self.host = host
        self.port = port
        self.retries = retries
        self.retryDelay = retryDelay
        self.recoveryDelay = recoveryDelay
        self.showRetries = showRetries
        #Stores pushed update events until api.py sends them to the dashboard
        self.events = []
        self.eventsLock = threading.Lock()
        self.subscriptionThread = None
        self.subscriptionRunning = False

    #Stores one publish-subscribe message received from the server
    def record_event(self, event):
        #Adds one server-pushed event to the local notification list
        with self.eventsLock:
            #Newest first means the latest write update is easiest to see
            self.events.insert(0, event)
            self.events = self.events[:100]

    #Returns recent server notifications for api.py to send to the browser
    def recent_events(self):
        #Gives api.py a safe copy of the events to return to the browser
        with self.eventsLock:
            return list(self.events)

    #Starts the background subscription thread if it is not already running
    def start_subscription(self, nodeId = "browser-client"):
        #Starts the background listener used for publish-subscribe updates
        #This check stops the same API process creating the listener twice
        if (self.subscriptionThread and self.subscriptionThread.is_alive()):
            return

        self.subscriptionRunning = True
        self.subscriptionThread = threading.Thread(
            target = self.subscription_loop,
            args = (nodeId,),
            daemon = True
        )
        self.subscriptionThread.start()

    #Keeps listening for server-pushed write updates and reconnects if needed
    def subscription_loop(self, nodeId):
        #Keeps a subscription socket open so the server can push write updates
        while self.subscriptionRunning:
            try:
                with socket.create_connection((self.host, self.port), timeout = 3) as sock:
                    #Only the first connection should time out, waiting for updates should not
                    sock.settimeout(None)
                    file = sock.makefile("rwb")
                    #This tells the server to keep this socket as a subscriber
                    message = {
                        "action": "subscribe",
                        "payload": {"node_id": nodeId}
                    }
                    file.write(json.dumps(message).encode("utf-8") + b"\n")
                    file.flush()

                    for line in file:
                        #The first message confirms subscription, later messages are write events
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
                #Keep a visible note of subscription problems instead of failing silently
                self.record_event({
                    "type": "subscription_error",
                    "resource": "server",
                    "user_id": nodeId,
                    "time": time.strftime("%H:%M:%S"),
                    "message": "Subscription disconnected: " + str(error)
                })
                #Pause before reconnecting so a down server is not hit constantly
                time.sleep(self.recoveryDelay)

    #Sends one normal action to the server and returns its JSON response
    def request(self, action, payload = None):
        #Sends one action to the server and waits for one response

        payload = payload or {}
        #The action name is what the server uses to choose the right operation
        message = {
            "action": action,
            "payload": payload
        }

        lastError = "Server unavailable"

        for attempt in range(1, self.retries + 1):
            try:
                if (self.showRetries):
                    print("DistRes client attempt " + str(attempt) + "/" + str(self.retries))

                #Each normal action gets its own short socket connection
                with socket.create_connection((self.host, self.port), timeout = 3) as sock:
                    file = sock.makefile("rwb")

                    #The newline marks the end of this JSON request for the server
                    file.write(json.dumps(message).encode("utf-8") + b"\n")
                    file.flush()

                    #Normal requests expect one JSON response back from the server
                    response = file.readline()
                    if not response:
                        return {"okay": False, "error": "No response from server"}

                    #The Flask route can return this dictionary straight to the browser
                    result = json.loads(response.decode("utf-8"))
                    if isinstance(result, dict):
                        result["attempt"] = attempt
                    return result

            except (OSError, json.JSONDecodeError) as error:
                #Retry connection problems before giving the UI a controlled error
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
