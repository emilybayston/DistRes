#Data layer read/write processing for credentials, audit records and shared files

import hashlib
import os
import sqlite3

import database


class UserCredentialDataAccess:
    #Processes SQL reads and writes for the user database

    def hash_password(self, password):
        #Hashes passwords before storage so plain text passwords are not written to SQLite
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def init(self):
        #Creates required database tables and default coursework users
        #This keeps database setup in the data layer instead of the UI or socket layer
        with database.writeLock:
            con = database.connection()
            schema = (
                "CREATE TABLE IF NOT EXISTS users ("
                "user_id TEXT PRIMARY KEY,"
                "username TEXT NOT NULL UNIQUE,"
                "role TEXT NOT NULL,"
                "password_hash TEXT NOT NULL,"
                "created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))"
                ");"
                "CREATE TABLE IF NOT EXISTS audit_log ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "user_id TEXT,"
                "username TEXT,"
                "category TEXT NOT NULL DEFAULT 'INFO',"
                "detail TEXT,"
                "timestamp TEXT NOT NULL DEFAULT(datetime('now','localtime'))"
                ");"
            )
            con.executescript(schema)
            con.commit()

            count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if (count == 0):
                #Default users make the demonstration repeatable without manual database setup
                defaults = [
                    ("ENG001", "Eng001", "Engineer"),
                    ("ENG002", "Eng002", "Engineer"),
                    ("ADM001", "Adm001", "Admin"),
                    ("ADM002", "Adm002", "Admin"),
                    ("ADM003", "Adm003", "Admin"),
                ]

                for userId, name, role in defaults:
                    con.execute(
                        "INSERT INTO users (user_id, username, role, password_hash) VALUES (?,?,?,?)",
                        (userId, name, role, self.hash_password(name))
                    )
                con.commit()
            con.close()

    def authenticate(self, user_id, password):
        #Reads one matching credential row for login validation
        #The application layer decides what to do if this returns no user
        con = database.connection()
        row = con.execute(
            "SELECT user_id, username, role "
            "FROM users "
            "WHERE user_id = ? AND password_hash = ?",
            (user_id.upper(), self.hash_password(password))
        ).fetchone()
        con.close()
        return dict(row) if row else None

    def list_users(self):
        #Reads all registered users for the management screen
        con = database.connection()
        rows = con.execute(
            "SELECT user_id, username, role, created_at "
            "FROM users "
            "ORDER BY user_id",
        ).fetchall()
        con.close()
        return [dict(row) for row in rows]

    def register_user(self, user_id, username, role, password):
        #Writes a new credential row and reports duplicate user errors
        with database.writeLock:
            con = database.connection()
            try:
                con.execute(
                    "INSERT INTO users (user_id, username, role, password_hash) VALUES (?,?,?,?)",
                    (user_id.upper(), username, role, self.hash_password(password))
                )
                con.commit()
                return True, ""
            except sqlite3.IntegrityError:
                return False, "Username or ID already exists."
            finally:
                con.close()

    def change_password(self, user_id, new_password):
        #Writes a new password hash for an existing user account
        with database.writeLock:
            con = database.connection()
            con.execute(
                "UPDATE users SET password_hash = ? WHERE user_id = ?",
                (self.hash_password(new_password), user_id.upper())
            )
            con.commit()
            con.close()

    def delete_user(self, user_id):
        #Deletes one credential row after the application layer checks session state
        with database.writeLock:
            con = database.connection()
            con.execute("DELETE FROM users WHERE user_id = ?", (user_id.upper(),))
            con.commit()
            con.close()

    def write_audit(self, user_id, username, category, detail):
        #Writes an audit record for a completed server-side event
        #Audit writes use the same lock as user writes to avoid overlapping SQLite commits
        with database.writeLock:
            con = database.connection()
            con.execute(
                "INSERT INTO audit_log (user_id, username, category, detail) VALUES (?,?,?,?)",
                (user_id, username, category, detail)
            )
            con.commit()
            con.close()

    def read_audit_log(self, limit = 250):
        #Reads recent audit records for the audit screen
        con = database.connection()
        rows = con.execute(
            "SELECT user_id, username, category, detail, timestamp "
            "FROM audit_log "
            "ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        con.close()
        return [dict(row) for row in rows]


class SharedFileDataAccess:
    #Processes physical reads and writes for distributed shared text files

    def __init__(self, basePath):
        #The server owns both shared resources, clients never open these files directly
        self.resourceFiles = {
            "ProductSpecification.txt": os.path.join(basePath, "ProductSpecification.txt"),
            "TeamNotes.txt": os.path.join(basePath, "TeamNotes.txt"),
        }

    def default_file(self, resource):
        #Creates starter content when a shared resource file does not exist
        return (
            resource + "\n\n"
            "Distributed Resource Access and Synchronisation Engine" + "\n\n"
            "Some text."
        )

    def ensure_files(self):
        #Creates missing shared resource files before clients can request them
        for resource, path in self.resourceFiles.items():
            if not os.path.exists(path):
                with open(path, "w", encoding = "utf-8") as file:
                    file.write(self.default_file(resource))

    def normalise_resource(self, resource):
        #Maps unknown resource names back to the default shared file
        name = (resource or "ProductSpecification.txt").strip().lower()
        if name not in self.resourceFiles:
            return "ProductSpecification.txt"
        return name

    def read_file(self, resource):
        #Reads the current contents of one shared resource file
        #Lock permission is checked before this method is called by the application layer
        resource = self.normalise_resource(resource)
        with open(self.resourceFiles[resource], "r", encoding = "utf-8") as file:
            return file.read()

    def write_file(self, resource, content):
        #Writes committed content to one shared resource file
        #Only the write lock owner should reach this method through DistRes
        resource = self.normalise_resource(resource)
        with open(self.resourceFiles[resource], "w", encoding = "utf-8") as file:
            file.write(content)

    def read_all_files(self):
        #Reads every shared resource so the dashboard can show current contents
        return {resource: self.read_file(resource) for resource in self.resourceFiles}

    def resources(self):
        #Returns the available distributed resource names
        return list(self.resourceFiles.keys())

userCredentialData = UserCredentialDataAccess()
