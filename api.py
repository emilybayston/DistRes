#Connect frontend and backend, connect to database and ConRes

import os
from flask import Flask, jsonify, request

import database
import conRes as concurrent

app = Flask(__name__)
conRes = concurrent.ConRes(capacity = 4)

htmlPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "interface.html")
with open(htmlPath, encoding = "utf-8") as file:
    html = file.read()

@app.route("/")
def index():
    return html

@app.route("/api/state")
def state():
    return jsonify(conRes.status())

@app.route("/api/login", methods = ["POST"])
def login():
    data = request.json
    return jsonify(conRes.login(data["user_id"], data["password"]))

@app.route("/api/logout", methods = ["POST"])
def logout():
    return jsonify(conRes.logout(request.json["user_id"]))

@app.route("/api/read", methods = ["POST"])
def read():
    return jsonify(conRes.acquire_read_lock(request.json["user_id"], request.json.get("resource", "product.txt")))

@app.route("/api/write", methods = ["POST"])
def write():
    return jsonify(conRes.acquire_write_lock(request.json["user_id"], request.json.get("resource", "product.txt")))

@app.route("/api/release", methods = ["POST"])
def release():
    return jsonify(conRes.release(request.json["user_id"], request.json.get("resource")))

@app.route("/api/commit", methods = ["POST"])
def commit():
    data = request.json
    return jsonify(conRes.commit_write(data["user_id"], data["content"], data.get("resource", "product.txt")))

@app.route("/api/users")
def users():
    return jsonify(database.list_users())

@app.route("/api/users/register", methods = ["POST"])
def register():
    data = request.json
    okay, error = database.register_user(
        data["user_id"], data["username"], data.get("role", "Engineer"), data["password"]
    )

    if (okay):
        conRes.log.add(
            "Registered: " + data["user_id"] + " (" + data["username"] + ")",
            "LOGIN"
        )
    return jsonify({"okay": okay, "error": error})

@app.route("/api/users/delete", methods=["POST"])
def delete():
    userId = request.json["user_id"]
    if (conRes.activeUsers.contains(userId)):
        return jsonify({ "okay": False, "error": userId + " is logged in. Logout first." })
    database.delete_user(userId)
    conRes.log.add("Deleted: " + userId, "WARN")
    return jsonify({ "okay": True })

@app.route("/api/users/change_password", methods=["POST"])
def change_password():
    data = request.json
    if (not database.authenticate(data["user_id"], data["current_password"])):
        return jsonify({ "okay": False, "error": "Current password incorrect." })
    database.change_password(data["user_id"], data["new_password"])
    conRes.log.add("Password Changed: " + data["user_id"], "INFO")
    return jsonify({ "okay": True })

@app.route("/api/audit_log")
def audit_log():
    return jsonify(database.read_audit_log())

@app.route("/api/reconfigure", methods = ["POST"])
def reconfigure():
    conRes.update_slots(int(request.json["capacity"]))
    return jsonify({"okay": True})

@app.route("/api/clear_log", methods = ["POST"])
def clear_log():
    conRes.log.clear()
    return jsonify({"okay": True})

if (__name__ == "__main__"):
    database.init()
    print("Open: http://127.0.0.1:5000")
    app.run(debug = False,
            threaded = True,
            host = "127.0.0.1",
            port = 5000)
