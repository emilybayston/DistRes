#Browser-facing API for the DistRes client layer
#The browser can make HTTP requests, but the distributed server uses sockets
#These routes translate UI actions into DistResClient socket requests
#Keeping this bridge thin means locking, authentication and file access stay server side

import os
from flask import Flask, jsonify, request
from client import DistResClient

#Flask owns the web routes only, not the shared files or user database
app = Flask(__name__)
#Used for normal user actions that should be sent to the DistRes socket server
distResClient = DistResClient()
#Used for quick dashboard health checks so polling does not block the interface
healthClient = DistResClient(retries = 1, retryDelay = 0, recoveryDelay = 0)
#Keeps one background subscription open for write update notifications
distResClient.start_subscription("flask-ui")

htmlPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "interface.html")
#Loads the client-layer interface from disk when Flask starts
with open(htmlPath, encoding = "utf-8") as file:
    html = file.read()

@app.route("/")
def index():
    #Returns the UI while keeping data access behind the server API
    return html

@app.route("/api/state")
def state():
    #Gets a fast server snapshot and includes events already pushed to this client
    serverState = healthClient.request("state")
    if isinstance(serverState, dict):
        serverState["clientEvents"] = distResClient.recent_events()
    return jsonify(serverState)

@app.route("/api/login", methods = ["POST"])
def login():
    #Forwards credentials so the server can authenticate against its database
    return jsonify(distResClient.request("login", request.json))

@app.route("/api/logout", methods = ["POST"])
def logout():
    #Lets the server release the session, semaphore slot and any held locks
    return jsonify(distResClient.request("logout", request.json))

@app.route("/api/read", methods = ["POST"])
def read():
    #Asks the server for a read lock before the UI treats the file as readable
    return jsonify(distResClient.request("read", request.json))

@app.route("/api/write", methods = ["POST"])
def write():
    #Asks the server for exclusive write access before editing is enabled
    return jsonify(distResClient.request("write", request.json))

@app.route("/api/release", methods = ["POST"])
def release():
    #Tells the server to clear this client's lock for the selected resource
    return jsonify(distResClient.request("release", request.json))

@app.route("/api/commit", methods = ["POST"])
def commit():
    #Sends edited text to the server where write ownership is checked again
    return jsonify(distResClient.request("commit", request.json))

@app.route("/api/users")
def users():
    #Reads user management data through the server to preserve the layer boundary
    return jsonify(distResClient.request("users"))

@app.route("/api/users/register", methods = ["POST"])
def register():
    #Creates users through the server so validation and audit logging stay centralised
    return jsonify(distResClient.request("register_user", request.json))

@app.route("/api/users/delete", methods=["POST"])
def delete():
    #Lets the server block deletion when the user is currently logged in
    return jsonify(distResClient.request("delete_user", request.json))

@app.route("/api/users/change_password", methods=["POST"])
def change_password():
    #Keeps password verification and updating inside the server-owned user flow
    return jsonify(distResClient.request("change_password", request.json))

@app.route("/api/audit_log")
def audit_log():
    #Fetches audit records through the same server route as other protected data
    return jsonify(distResClient.request("audit_log"))

@app.route("/api/reconfigure", methods = ["POST"])
def reconfigure():
    #Sends capacity changes to the server semaphore controller
    return jsonify(distResClient.request("reconfigure", request.json))

@app.route("/api/clear_log", methods = ["POST"])
def clear_log():
    #Clears the server event log rather than only clearing the browser display
    return jsonify(distResClient.request("clear_log", request.json))

if (__name__ == "__main__"):
    #Runs the browser-facing API while server.py runs the socket server separately
    print("DistRes UI: http://127.0.0.1:5000")
    app.run(debug = False,
            threaded = True,
            host = "127.0.0.1",
            port = 5000)
