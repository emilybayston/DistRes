#Coordinates user sessions, resource lock ownership, shared file access and event logging

import threading
import queue
import time
import os
from datetime import datetime
from collections import OrderedDict

import database

class DistRes:
    #Coordinates login capacity, active users and safe access to shared resource files

    def __init__(self, capacity = 4):
        self.capacity = capacity
        self.semaphoreControl = SemaphoreController(capacity)
        self.activeUsers = UserManagement()    #Stores logged-in users, roles, session ids and activity state
        self.waitingUsers = WaitingUserQueue()    #Stores login sessions waiting for a free server capacity slot
        self.threadManager = ThreadManager()
        self.log = EventLog()

        self.resourceFiles = {
            "product.txt": os.path.join(os.path.dirname(os.path.abspath(__file__)), "product.txt"),
            "teamnotes.txt": os.path.join(os.path.dirname(os.path.abspath(__file__)), "teamnotes.txt"),
        }
        self.rwLocks = {resource: RWLock() for resource in self.resourceFiles}
        self.fileActivity = {resource: UserAccessTracker() for resource in self.resourceFiles}
        self.ensure_resource_files()
        #Records each active user's resource locks so duplicate file locks can be blocked
        #Example: {user_id: {"product.txt": "READ", "teamnotes.txt": "WRITE"}}
        self.userResources = {}
        self.userResourcesLock = threading.Lock()
        self.lock = threading.Lock()    #Serialises physical file writes so committed content is not interleaved

        self.info = dict(logins = 0, reads = 0, writes = 0, blocked = 0)
        self.infoLock = threading.Lock()
        
    def defaultFile(self, resource):
        return (
            resource + '\n\n'
            "Distributed Resource Access and Synchronisation Engine" + '\n\n'
            "Some text."
        )

    def ensure_resource_files(self):
        for resource, path in self.resourceFiles.items():
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as file:
                    file.write(self.defaultFile(resource))

    def normalise_resource(self, resource):
        name = (resource or "product.txt").strip().lower()
        if name not in self.resourceFiles:
            return "product.txt"
        return name

    def read_file(self, resource):
        resource = self.normalise_resource(resource)
        with open(self.resourceFiles[resource], "r", encoding="utf-8") as file:
            return file.read()

    def write_file(self, resource, content):
        resource = self.normalise_resource(resource)
        with open(self.resourceFiles[resource], "w", encoding="utf-8") as file:
            file.write(content)

    def locked_resource(self):
        with self.userResourcesLock:
            for locks in self.userResources.values():
                for resource in locks:
                    return resource
            return None

    def user_locked_resources(self, user_id):
        with self.userResourcesLock:
            return dict(self.userResources.get(user_id, {}))

    def first_user_resource(self, user_id):
        with self.userResourcesLock:
            for resource in self.userResources.get(user_id, {}):
                return resource
            return None

    def user_holds_lock(self, user_id):
        with self.userResourcesLock:
            return bool(self.userResources.get(user_id))

    def user_resource(self, user_id, resource):
        resource = self.normalise_resource(resource)
        with self.userResourcesLock:
            return self.userResources.get(user_id, {}).get(resource)

    def reserve_user_resource(self, user_id, resource, mode):
        #Reserves ownership before waiting on the RWLock so duplicate requests fail early
        #Allows one user to read different files while blocking repeated reads of the same file
        resource = self.normalise_resource(resource)
        with self.userResourcesLock:
            locks = self.userResources.setdefault(user_id, {})
            if resource in locks:
                return False, resource
            if mode == "WRITE" and locks:
                return False, ", ".join(locks.keys())
            if mode == "READ" and "WRITE" in locks.values():
                return False, ", ".join(locks.keys())
            locks[resource] = mode
            return True, resource

    def clear_user_resource(self, user_id, resource = None):
        with self.userResourcesLock:
            if resource is None:
                return self.userResources.pop(user_id, None)
            locks = self.userResources.get(user_id)
            if not locks:
                return None
            removed = locks.pop(resource, None)
            if not locks:
                self.userResources.pop(user_id, None)
            return removed
    
    def availability(self):
        return self.semaphoreControl.availability(self.activeUsers.count())
    
    def info_increment(self, key):
        with self.infoLock:
            self.info[key] += 1

    def waiting_list_peek(self):
        return self.waitingUsers.items()
    
    def login(self, user_id, password):
        user = database.authenticate(user_id, password)
        if (not user):
            self.log.add(f"Authentication Failed: {user_id} - invalid user details.", "WARN", user_id)
            return {"okay": False, "error": "Invalid user ID or password."}
        id, name, role = user["user_id"], user["username"], user["role"]

        if (self.activeUsers.contains(id)):    #Prevents the same user id from opening two active sessions
            return {"okay": False, "error": f"{user_id} already logged in."}
        for user in self.waiting_list_peek():
            if (user["user_id"] == id):
                return {"okay": False, "error": f"{user_id} already in queue."}
            
        threadId = self.threadManager.next_session_id()
        session = {
            "user_id": id, "username": name, "role": role,
            "threadId": threadId, "state": "IDLE",
            "login_at": datetime.now().strftime("%H:%M:%S"),
        }

        if (self.semaphoreControl.acquire_now()):  #Admits the user immediately when the server has spare capacity
            self.allow(session)
            return {"okay": True, "status": "active", "username": name, "role": role}
        
        position = self.waitingUsers.put(session)  #Queues the session and returns its FIFO queue position
        self.info_increment("blocked")
        self.log.add(
            f"Blocked: {id} ({name}) - full capacity"
            f"[{self.capacity}/{self.capacity}]. Queue position: {position}",
            "BLOCK", id, name
        )
        #Creates a waiting thread for a queued login without blocking the UI request
        #The thread sleeps on the semaphore until logout releases a capacity slot
        self.threadManager.start_waiter(self.wait_entry, session, id)
        return {"okay": True, "status": "queued", "username": name, "role": role, "position": position}
    
    def wait_entry(self, session):  #Waits for a released capacity slot and promotes the queued session
        self.semaphoreControl.wait()
        self.waitingUsers.remove(session["user_id"])

        self.allow(session, pushed=True)

    def allow(self, session, pushed = False):   #Adds a session to active users and records admission details
        self.activeUsers.add(session["user_id"], session)
        self.info_increment("logins")
        verb = "Pushed" if pushed else "Login"
        self.log.add(
            f"{verb}: {session['user_id']} ({session['username']}) "
            f"[{session['role']}] - Session {session['threadId']} active. "
            f"Semaphore: {self.availability()}/{self.capacity}",
            "LOGIN", session["user_id"], session["username"]
        )
    
    def logout(self, user_id):
        session = self.activeUsers.remove(user_id)
        if (not session):
            return {"okay": False, "error": "User not in active sessions."}

        for heldResource, mode in self.user_locked_resources(user_id).items():
            if (mode == "READ" and self.fileActivity[heldResource].is_reader(user_id)):    #Releases every read lock held by the user during logout
                self.rwLocks[heldResource].release_read_lock(user_id)
                self.fileActivity[heldResource].remove_reader(user_id)
                self.clear_user_resource(user_id, heldResource)
                self.log.add(f"Released: {user_id} read lock released for {heldResource}.", "READ", user_id, session["username"])

            if (mode == "WRITE" and self.fileActivity[heldResource].is_writer(user_id)):
                self.rwLocks[heldResource].release_write_lock(user_id)
                self.fileActivity[heldResource].clear_writer(user_id)
                self.clear_user_resource(user_id, heldResource)
                self.log.add(f"Released: {user_id} write lock released for {heldResource}.", "WRITE", user_id, session["username"])

        self.semaphoreControl.release()    #Frees one capacity slot and wakes the next waiting session
        self.log.add(
            f"Logout: {user_id} ({session['username']}) - "
            f"Session {session['threadId']} terminated. Semaphore released.",
            "OUT", user_id, session["username"]
        )
        return {"okay": True}

    def acquire_read_lock(self, user_id, resource = "product.txt"):
        session = self.activeUsers.get(user_id)  #Rejects resource access unless the user has an active session
        if (not session):
            return {"okay": False, "error": "User not logged in."}
        resource = self.normalise_resource(resource)

        reserved, heldResource = self.reserve_user_resource(user_id, resource, "READ")
        if (not reserved):
            return {"okay": False, "error": f"{user_id} already holds a lock for {heldResource}."}

        self.log.add(f"Read request: {user_id} acquiring shared read lock for {resource}...", "READ", user_id, session["username"])

        rwLock = self.rwLocks[resource]
        fileActivity = self.fileActivity[resource]
        okay = rwLock.acquire_read_lock(user_id, timeout = 5) #Allows shared read access unless a writer holds the file

        if (okay):
            self.activeUsers.update_state(user_id, "READING")
            fileActivity.add_reader(user_id)
            self.info_increment("reads")
            state = rwLock.status()
            self.log.add(
                f"Read lock: {user_id} - {resource} lock acquired. "
                f"{len(state['readers'])} concurrent reader(s).",
                "READ", user_id, session["username"]
            )
        else:
            self.clear_user_resource(user_id, resource)
            self.log.add(
                f"Read blocked: {user_id} - writer {rwLock.writerId} "
                f"holds {resource}. Timeout 5s. Deadlock avoidance.",
                "WARN", user_id, session["username"]
            )
        return {"okay": okay, "error": "" if okay else "Read blocked - writer active (5s timeout)."}
    
    def acquire_write_lock(self, user_id, resource = "product.txt"):
        session = self.activeUsers.get(user_id)
        if (not session):
            return {"okay": False, "error": "User not logged in."}
        resource = self.normalise_resource(resource)

        reserved, heldResource = self.reserve_user_resource(user_id, resource, "WRITE")
        if (not reserved):
            return {"okay": False, "error": f"{user_id} already holds a lock for {heldResource}. Release it first."}

        self.log.add(f"Write Request: {user_id} acquiring exclusive write lock for {resource}...", "WRITE", user_id, session["username"])

        rwLock = self.rwLocks[resource]
        fileActivity = self.fileActivity[resource]
        okay = rwLock.acquire_write_lock(user_id, timeout = 5)

        if (okay):
            self.activeUsers.update_state(user_id, "WRITING")
            fileActivity.set_writer(user_id)
            self.info_increment("writes")
            self.log.add(
                f"Write lock: {user_id} - exclusive lock for {resource}. All other readers/writers blocked.",
                "WRITE", user_id, session["username"]
            )
        else:
            self.clear_user_resource(user_id, resource)
            state = rwLock.status()
            reason = (f"writer {state['writer']} holds lock" if state["writer"]
                      else f"{state['readerCount']} reader(s) active")
            self.log.add(
                f"Write blocked: {user_id} - {reason}. "
                f"Consistent lock order enforced. Deadlock prevented.",
                "WARN", user_id, session["username"]
            )
        return {"okay": okay, "error": "" if okay else "Write blocked - resource busy (5s timeout)."}
    
    def release(self, user_id, resource = None):
        session = self.activeUsers.get(user_id)
        released = False
        resource = self.normalise_resource(resource) if resource else self.first_user_resource(user_id)

        if (resource and self.fileActivity[resource].is_reader(user_id)):    #Releases only this user's read lock for the selected resource
            self.rwLocks[resource].release_read_lock(user_id)
            self.fileActivity[resource].remove_reader(user_id)
            self.clear_user_resource(user_id, resource)
            if (not self.user_holds_lock(user_id)):
                self.activeUsers.update_state(user_id, "IDLE")
            state = self.rwLocks[resource].status()
            self.log.add(
                f"Read lock released: {user_id} exited {resource}. {state['readerCount']} reader(s) remain.",
                "READ", user_id, session["username"] if session else None
            )
            released = True

        if (resource and self.fileActivity[resource].is_writer(user_id)):    #Releases only this user's write lock for the selected resource
            self.rwLocks[resource].release_write_lock(user_id)
            self.fileActivity[resource].clear_writer(user_id)
            self.clear_user_resource(user_id, resource)
            if (not self.user_holds_lock(user_id)):
                self.activeUsers.update_state(user_id, "IDLE")
            self.log.add(
                f"Write lock released: {user_id} exited {resource} - resource now available.",
                "WRITE", user_id, session["username"] if session else None
            )
            released = True

        if (not released):
            return {"okay": False, "error": "No lock held by this user."}
        return {"okay": True}

    def commit_write(self, user_id, newContent, resource = "product.txt"):
        session = self.activeUsers.get(user_id)
        resource = self.normalise_resource(resource)

        if(self.user_resource(user_id, resource) != "WRITE" or not self.fileActivity[resource].is_writer(user_id)):
            return {"okay": False, "error": "Only the user holding the write lock can commit this file."}

        with self.lock:
            self.write_file(resource, newContent)

        self.log.add(
            f"Write commit: {user_id} saved changes to {resource}.",
            "WRITE", user_id, session["username"] if session else None
        )
        return {"okay": True, "resource": resource}
    
    def update_slots(self, n):
        oldCapacity = self.capacity
        availability = self.semaphoreControl.reconfigure(n, self.activeUsers.count())
        self.capacity = self.semaphoreControl.capacity
        self.log.add(
            f"Reconfigured: Semaphore N {oldCapacity} -> {n}. Available = {availability}", "SYS")
    
    def status(self):   #Builds a read-only snapshot for dashboard polling and lock visualisation
        activeUsers = self.activeUsers.status()
        waitingUsers = self.waiting_list_peek()
        rw = {resource: lock.status() for resource, lock in self.rwLocks.items()}
        for resource, state in rw.items():
            state["resource"] = resource
        selectedResource = self.locked_resource() or "product.txt"
        selectedRw = dict(rw[selectedResource])
        fileActivity = {resource: tracker.status() for resource, tracker in self.fileActivity.items()}
        log = self.log.status(100)

        with self.lock:
            files = {resource: self.read_file(resource) for resource in self.resourceFiles}

        with self.infoLock:
            info = dict(self.info)

        return {
            "activeUsers":    activeUsers,
            "waitingUsers":   waitingUsers,
            "semaphore": self.semaphoreControl.status(self.activeUsers.count()),
            "rw":          selectedRw,
            "rwByResource": rw,
            "fileActivity": fileActivity,
            "resources":   list(self.resourceFiles.keys()),
            "files":       files,
            "file":        files["product.txt"],
            "log":        log,
            "info":       info,
        }

class SemaphoreController:
    #Controls server session capacity and isolates semaphore operations from login logic

    def __init__(self, capacity):
        self.capacity = capacity
        self.semaphore = threading.Semaphore(capacity)
        self.lock = threading.Lock()

    def availability(self, active_count):
        with self.lock:
            return max(0, self.capacity - active_count)

    def acquire_now(self):
        return self.semaphore.acquire(blocking = False)

    def wait(self):
        self.semaphore.acquire()

    def release(self):
        self.semaphore.release()

    def reconfigure(self, capacity, active_count):
        with self.lock:
            self.capacity = capacity
            available = max(0, capacity - active_count)
            self.semaphore = threading.Semaphore(available)
            return available

    def status(self, active_count):
        with self.lock:
            available = max(0, self.capacity - active_count)
            return {
                "available": available,
                "capacity": self.capacity,
                "occupied": active_count,
            }


class ThreadManager:
    #Creates unique session ids and background waiter threads for queued users

    def __init__(self):
        self.counter = 1
        self.lock = threading.Lock()

    def next_session_id(self):
        with self.lock:
            thread_id = f"Thread{self.counter}"
            self.counter += 1
            return thread_id

    def start_waiter(self, target, session, user_id):
        thread = threading.Thread(
            target = target,
            args = (session,),
            daemon = True,
            name = f"DistResWaiter{user_id}"
        )
        thread.start()
        return thread


class WaitingUserQueue:
    #Wraps the FIFO queue so waiting-user operations stay outside the main coordinator

    def __init__(self):
        self.queue = queue.Queue(maxsize = 0)

    def put(self, session):
        self.queue.put(session)
        return self.queue.qsize()

    def items(self):
        with self.queue.mutex:
            return list(self.queue.queue)

    def remove(self, user_id):
        with self.queue.mutex:
            items = [s for s in self.queue.queue if s["user_id"] != user_id]
            self.queue.queue.clear()
            self.queue.queue.extend(items)
            self.queue.not_empty.notify_all()
            self.queue.not_full.notify_all()


class RWLock:
    #Allows multiple concurrent readers or one exclusive writer for a single resource
    def __init__(self):
        self.condition = threading.Condition(threading.Lock())
        self.readerCount = 0
        self.writerLock = threading.Lock()
        self.writerId = None
        self.readerIds = set()
        self.stateLock = threading.Lock()   #Protects writer and reader identifiers returned to the dashboard

    def acquire_read_lock(self, user_id, timeout = 5):
        timer = time.time() + timeout   #Limits how long a read request waits behind an active writer
        with self.condition:
            if user_id in self.readerIds:
                return False

            while (self.writerId is not None):
                remainingTime = timer - time.time()
                if (remainingTime <= 0):
                    return False
                self.condition.wait(timeout = remainingTime)

            #Records this user as a current reader after confirming no writer is active
            self.readerCount += 1
            self.readerIds.add(user_id)
            return True
        
    def release_read_lock(self, user_id):
        with self.condition:
            if user_id not in self.readerIds:
                return
            self.readerIds.discard(user_id)
            self.readerCount = max(0, self.readerCount - 1)
            #Wakes waiting writers only when the final reader leaves the resource
            if (self.readerCount == 0):
                self.condition.notify_all()

    def acquire_write_lock(self, user_id, timeout = 5):
        if not (self.writerLock.acquire(timeout = timeout)):    #Times out if another writer already owns the writer gate
            return False
        timer = time.time() + timeout
        with self.condition:
            while (self.readerCount > 0):   #Waits until all active readers leave before granting write access
                remainingTime = timer - time.time()
                if (remainingTime <= 0):
                    self.writerLock.release()   #Releases writer gate if readers do not leave before timeout
                    return False
                self.condition.wait(timeout = remainingTime)
            self.writerId = user_id
            return True
        
    def release_write_lock(self, user_id):
        with self.condition:
            if (self.writerId != user_id):
                return False
            self.writerId = None
            self.condition.notify_all()
            try:
                self.writerLock.release()
            except RuntimeError:
                pass
            return True
        
    def status(self):
        with self.condition:
            return {
                "readerCount": self.readerCount,
                "readers": list(self.readerIds),
                "writer": self.writerId,
            }

    
class UserManagement:
    #Thread-safe active user and session store used by login, logout and dashboard polling

    def __init__(self):
        self.store = OrderedDict()    #Stores logged-in users in session order for stable UI display
        self.lock = threading.Lock()    #Protects the session store during add, remove and state updates
    
    def add(self, user_id, session):
        with self.lock:
            self.store[user_id] = session
    
    def remove(self, user_id):
        with self.lock:
            return self.store.pop(user_id)
    
    def get(self, user_id):
        with self.lock:
            return self.store.get(user_id)
        
    def contains(self, user_id):
        with self.lock:
            return user_id in self.store
    
    def update_state(self, user_id, state):
        with self.lock:
            if (user_id in self.store):
                self.store[user_id]["state"] = state    #Updates the user's activity state shown in the dashboard
    
    def count(self):
        with self.lock:
            return len(self.store)
    
    def status(self):
        return list(self.store.values())


CurrentSessions = UserManagement

class UserAccessTracker:    #Tracks current readers and writer for one resource
    def __init__(self):
        self.lock = threading.Lock()
        self.readerIds = set()
        self.writerId = None

    def add_reader(self, user_id):
        with self.lock:
            self.readerIds.add(user_id)
    
    def remove_reader(self, user_id):
        with self.lock:
            self.readerIds.discard(user_id)
    
    def set_writer(self, user_id):
        with self.lock:
            self.writerId = user_id
    
    def clear_writer(self, user_id):
        with self.lock:
            if (self.writerId == user_id):
                self.writerId = None
    
    def is_reader(self, user_id):
        with self.lock:
            return user_id in self.readerIds
    
    def is_writer(self, user_id):
        with self.lock:
            return self.writerId == user_id
        
    def status(self):
        with self.lock:
            return {
                "readerCount": len(self.readerIds),
                "readers": list(self.readerIds),
                "writer": self.writerId, 
            }

    def clear_user(self, user_id):   #Removes a user from reader and writer tracking during cleanup
        with self.lock:
            self.readerIds.discard(user_id)
            if (self.writerId == user_id):
                self.writerId = None

class EventLog():
    def __init__(self):
        self.entries = []
        self.lock = threading.Lock()
    
    def add(self, message, category = "INFO", user_id = None, username = None):
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "message": message,
            "category": category,
        }
    
        with self.lock:
            self.entries.insert(0, entry)
            if (len(self.entries) > 300):   #Keeps the in-memory log bounded to avoid unbounded growth
                self.entries.pop()

        if (category not in ("SYS",) and user_id is not None):
            thread = threading.Thread(
                target = database.write_audit,
                args = (user_id, username, category, message),
                daemon = True   #Writes audit entries in the background without delaying user actions
            )
            thread.start()

    def status(self, limit = 150):
        with self.lock:
            return list(self.entries[:limit])
    
    def clear(self):
        with self.lock:
            self.entries.clear()


#Keeps older code paths constructing the same engine class
ConRes = DistRes
