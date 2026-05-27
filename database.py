#Database Layer
#Handles all SQLite operations including registration, authentication and updating user details

import sqlite3
import hashlib
import os
import threading


dbPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "consres.db")

writeLock = threading.Lock()

def connection():
    con = sqlite3.connect(dbPath, check_same_thread = False)    #Allow multiple threads to access
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")    #Allow multiple threads to read from database at same time
    con.execute("PRAGMA foreign_keys = ON")
    return con

def init():
    with writeLock:
        con = connection()
        con.executescript("""
                        CREATE TABLE IF NOT EXISTS users (
                          user_id   TEXT PRIMARY KEY,
                          username  TEXT NOT NULL UNIQUE,
                          role  TEXT NOT NULL,
                          password_hash TEXT NOT NULL,
                          created_at    TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
                          );

                        CREATE TABLE IF NOT EXISTS audit_log (
                          id    INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id   TEXT,
                          username  TEXT,
                          category TEXT NOT NULL DEFAULT 'INFO',
                          detail    TEXT,
                          timestamp TEXT NOT NULL DEFAULT(datetime('now','localtime'))
                          );
                        """)
        con.commit()

        #Check if empty, insert dummy data if true
        count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if (count == 0):
            defaults = [
                ("ENG001", "Eng001", "Engineer"),
                ("ENG002", "Eng002", "Engineer"),
                ("ADM001", "Adm001", "Admin"),
                ("ADM002", "Adm002", "Admin"),
                ("ADM003", "Adm003", "Admin"),
            ]    

            for id, name, role in defaults:
                con.execute(
                    "INSERT INTO users (user_id, username, role, password_hash) VALUES (?,?,?,?)",
                    (id, name, role, hash_password(name))
                )  
            con.commit()
    con.close()     


#Functions for user management
def authenticate(user_id, password):
    con = connection()
    row = con.execute(
        "SELECT user_id, username, role "
        "FROM users "
        "WHERE user_id = ? AND password_hash = ?",  #Placeholders for security, prevents SQL injection
        (user_id.upper(), hash_password(password))
    ).fetchone()
    con.close()
    return dict(row) if row else None

def get_user(user_id):
    con = connection()
    row = con.execute(
        "SELECT user_id, username, role, created_at "
        "FROM users "
        "WHERE user_id = ?",
        (user_id.upper(),)
    ).fetchone()
    con.close()
    return dict(row) if row else None

def list_users():
    con = connection()
    rows = con.execute(
        "SELECT user_id, username, role, created_at "
        "FROM users "
        "ORDER BY user_id",
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]

def register_user(user_id, username, role, password):
    with writeLock:
        con = connection()
        try:
            con.execute(
                "INSERT INTO users (user_id, username, role, password_hash) VALUES (?,?,?,?)",
                (user_id.upper(), username, role, hash_password(password))
            )
            con.commit()
            return True, ""
        except sqlite3.IntegrityError:
            return False, "Username or ID already exists."
        finally:
            con.close()

def change_password(user_id, new_password):
    with writeLock:
        con = connection()
        con.execute(
            "UPDATE users SET password_hash = ? WHERE user_id = ?",
            (hash_password(new_password), user_id.upper())
        )
        con.commit()
        con.close()
    
def delete_user(user_id):
    with writeLock:
        con = connection()
        con.execute("DELETE FROM users WHERE user_id = ?",(user_id.upper(),))
        con.commit()
        con.close()
 

def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


#Functions for audit log
def write_audit(user_id, username, category, detail):
    with writeLock:
        con = connection()
        con.execute(
            "INSERT INTO audit_log (user_id, username, category, detail) VALUES (?,?,?,?)",
            (user_id, username, category, detail)
        )
        con.commit()
        con.close()


def read_audit_log(limit = 250):
    con = connection()
    rows = con.execute(
        "SELECT user_id, username, category, detail, timestamp "
        "FROM audit_log "
        "ORDER BY id DESC LIMIT ?",
        (limit,)
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]
