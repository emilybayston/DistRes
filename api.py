#Routes browser requests to the DistRes coordination engine

import os
from flask import Flask, jsonify, request

from client import DistResClient

#Flask serves the browser UI, but does not own the shared resources
app = Flask(__name__)
#This client forwards user actions to the socket server as JSON RPC-style requests
distResClient = DistResClient()
#State checks use one fast attempt so the UI can show retry messages itself
healthClient = DistResClient(retries = 1, retryDelay = 0, recoveryDelay = 0)
#The Flask process keeps one subscription open for server write notifications
distResClient.start_subscription("flask-ui")

htmlPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "interface.html")
#The interface is loaded once so the browser layer stays as a plain HTML UI
with open(htmlPath, encoding = "utf-8") as file:
    html = file.read()

@app.route("/")
def index():
    #Returns the client-layer UI without giving it direct file or database access
    return html

@app.route("/api/state")
def state():
    #Polls the server for dashboard state and adds any pushed pub-sub events
    serverState = healthClient.request("state")
    if isinstance(serverState, dict):
        serverState["clientEvents"] = distResClient.recent_events()
    return jsonify(serverState)

@app.route("/api/login", methods = ["POST"])
def login():
    #Login is forwarded to the server so authentication stays server-side
    return jsonify(distResClient.request("login", request.json))

@app.route("/api/logout", methods = ["POST"])
def logout():
    #Logout releases server-side session capacity and any held resource locks
    return jsonify(distResClient.request("logout", request.json))

@app.route("/api/read", methods = ["POST"])
def read():
    #Read requests go through the socket server before file content is exposed
    return jsonify(distResClient.request("read", request.json))

@app.route("/api/write", methods = ["POST"])
def write():
    #Write requests ask the application layer for an exclusive lock
    return jsonify(distResClient.request("write", request.json))

@app.route("/api/release", methods = ["POST"])
def release():
    #Release tells the server which user's selected resource lock should be cleared
    return jsonify(distResClient.request("release", request.json))

@app.route("/api/commit", methods = ["POST"])
def commit():
    #Commit sends edited text to the server, where write ownership is checked again
    return jsonify(distResClient.request("commit", request.json))

@app.route("/api/users")
def users():
    #User management data is read through the server instead of directly from Flask
    return jsonify(distResClient.request("users"))

@app.route("/api/users/register", methods = ["POST"])
def register():
    #Registration is passed to the application layer before the data layer writes it
    return jsonify(distResClient.request("register_user", request.json))

@app.route("/api/users/delete", methods=["POST"])
def delete():
    #Deletion is blocked by the server if that user still has an active session
    return jsonify(distResClient.request("delete_user", request.json))

@app.route("/api/users/change_password", methods=["POST"])
def change_password():
    #Password changes are handled server-side so credential checks stay centralised
    return jsonify(distResClient.request("change_password", request.json))

@app.route("/api/audit_log")
def audit_log():
    #Audit records are fetched from the server-owned database through the socket API
    return jsonify(distResClient.request("audit_log"))

@app.route("/api/reconfigure", methods = ["POST"])
def reconfigure():
    #Capacity changes are applied by the server semaphore controller
    return jsonify(distResClient.request("reconfigure", request.json))

@app.route("/api/clear_log", methods = ["POST"])
def clear_log():
    #Clearing the visible event log is still routed through the server coordinator
    return jsonify(distResClient.request("clear_log", request.json))

if (__name__ == "__main__"):
    #Runs the browser-facing API while the socket server runs in a separate terminal
    print("DistRes UI: http://127.0.0.1:5000")
    app.run(debug = False,
            threaded = True,
            host = "127.0.0.1",
            port = 5000)
