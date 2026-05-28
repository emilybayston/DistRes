#Socket client wrapper for DistRes
import json
import socket
import time


class DistResClient:
    #Handles communication from the client side to the DistRes server

    def __init__(self, host = "127.0.0.1", port = 5050, retries = 3):
        self.host = host
        self.port = port
        self.retries = retries

    def request(self, action, payload = None):
        #Builds a request message and sends it to the server

        payload = payload or {}
        message = {
            "action": action,
            "payload": payload
        }

        lastError = "Server unavailable"

        for attempt in range(self.retries):
            try:
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

                    return json.loads(response.decode("utf-8"))

            except OSError as error:
                #If the server is not ready, retry a few times
                lastError = str(error)
                time.sleep(0.3)

        return {
            "okay": False,
            "error": "Could not connect to DistRes server: " + lastError
        }
