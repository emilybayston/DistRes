#Main concurrency engine

import threading
import queue
import time
import os
from datetime import datetime
from collections import OrderedDict

import database

#Main engine class - ConRes core functionality
class ConRes:
    def __init__(self, capacity = 4):
        self.capacity = capacity
        self.semaphore = threading.Semaphore(capacity)  #Counting semaphore, initial capacity at 4
        self.activeUsers = CurrentSessions()    #Logged in users
        self.waitingUsers = queue.Queue(maxsize = 0)    #FIF queue for threads waiting for semaphore
        self.log = EventLog()

        self.resourceFiles = {
            "product.txt": os.path.join(os.path.dirname(os.path.abspath(__file__)), "product.txt"),
            "teamnotes.txt": os.path.join(os.path.dirname(os.path.abspath(__file__)), "teamnotes.txt"),
        }
        self.rwLocks = {resource: RWLock() for resource in self.resourceFiles}
        self.fileActivity = {resource: UserAccessTracker() for resource in self.resourceFiles}
        self.ensure_resource_files()
        self.userResources = {}
        self.userResourcesLock = threading.Lock()
        self.lock = threading.Lock()    #File content write lock

        self.info = dict(logins = 0, reads = 0, writes = 0, blocked = 0)
        self.infoLock = threading.Lock()

        self.threads = 1    #Thread counter
        
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
        return max(0, self.capacity - self.activeUsers.count())
    
    def info_increment(self, key):
        with self.infoLock:
            self.info[key] += 1

    def waiting_list_peek(self):
        with self.waitingUsers.mutex:
            return list(self.waitingUsers.queue)
    
    def login(self, user_id, password):
        user = database.authenticate(user_id, password)
        if (not user):
            self.log.add(f"Authentication Failed: {user_id} - invalid user details.", "WARN", user_id)
            return {"okay": False, "error": "Invalid user ID or password."}
        id, name, role = user["user_id"], user["username"], user["role"]

        if (self.activeUsers.contains(id)):    #Prevent dupl users
            return {"okay": False, "error": f"{user_id} already logged in."}
        for user in self.waiting_list_peek():
            if (user["user_id"] == id):
                return {"okay": False, "error": f"{user_id} already in queue."}
            
        threadId = f"Thread{self.threads}"; 
        self.threads += 1
        session = {
            "user_id": id, "username": name, "role": role,  #Build session dict for user
            "threadId": threadId, "state": "IDLE",
            "login_at": datetime.now().strftime("%H:%M:%S"),
        }

        if (self.semaphore.acquire(blocking = False)):  #Try to acquire semaphore
            self.allow(session)
            return {"okay": True, "status": "active", "username": name, "role": role}
        
        self.waitingUsers.put(session)  #Add to queue if no room
        position = self.waitingUsers.qsize()
        self.info_increment("blocked")
        self.log.add(
            f"Blocked: {id} ({name}) - full capacity"
            f"[{self.capacity}/{self.capacity}]. Queue position: {position}",
            "BLOCK", id, name
        )
        thread = threading.Thread(  #Block semaphore acquire until room
            target = self.wait_entry,
            args = (session,),
            daemon = True,
            name = f"ConResWaiter{id}"
        )
        thread.start()
        return {"okay": True, "status": "queued", "username": name, "role": role, "position": position}
    
    def wait_entry(self, session):  #Wait for free user slot
        self.semaphore.acquire()

        with self.waitingUsers.mutex:  #Remove from queue by rebuild, no remove function
            items = [s for s in self.waitingUsers.queue
                     if s["user_id"] != session["user_id"]]
            self.waitingUsers.queue.clear()
            self.waitingUsers.queue.extend(items)
            self.waitingUsers.not_empty.notify_all()
            self.waitingUsers.not_full.notify_all()

        self.allow(session, pushed=True)

    def allow(self, session, pushed = False):   #Add to active sessions and log event
        self.activeUsers.add(session["user_id"], session)
        self.info_increment("logins")
        verb = "Pushed" if pushed else "Login"
        self.log.add(
            f"{verb}: {session['user_id']} ({session['username']}) "
            f"[{session['role']}] - Thread {session['threadId']} active. "
            f"Semaphore: {self.availability()}/{self.capacity}",
            "LOGIN", session["user_id"], session["username"]
        )
    
    def logout(self, user_id):
        session = self.activeUsers.remove(user_id)
        if (not session):
            return {"okay": False, "error": "User not in active sessions."}

        for heldResource, mode in self.user_locked_resources(user_id).items():
            if (mode == "READ" and self.fileActivity[heldResource].is_reader(user_id)):    #Auto release locks
                self.rwLocks[heldResource].release_read_lock(user_id)
                self.fileActivity[heldResource].remove_reader(user_id)
                self.clear_user_resource(user_id, heldResource)
                self.log.add(f"Released: {user_id} read lock released for {heldResource}.", "READ", user_id, session["username"])

            if (mode == "WRITE" and self.fileActivity[heldResource].is_writer(user_id)):
                self.rwLocks[heldResource].release_write_lock(user_id)
                self.fileActivity[heldResource].clear_writer(user_id)
                self.clear_user_resource(user_id, heldResource)
                self.log.add(f"Released: {user_id} write lock released for {heldResource}.", "WRITE", user_id, session["username"])

        self.semaphore.release()    #Release to wake waiting threads
        self.log.add(
            f"Logout: {user_id} ({session['username']}) - "
            f"Thread {session['threadId']} terminated. Semaphore released.",
            "OUT", user_id, session["username"]
        )
        return {"okay": True}

    def acquire_read_lock(self, user_id, resource = "product.txt"):
        session = self.activeUsers.get(user_id)  #Check user logged in
        if (not session):
            return {"okay": False, "error": "User not logged in."}
        resource = self.normalise_resource(resource)

        reserved, heldResource = self.reserve_user_resource(user_id, resource, "READ")
        if (not reserved):
            return {"okay": False, "error": f"{user_id} already holds a lock for {heldResource}."}

        self.log.add(f"Read request: {user_id} acquiring shared read lock for {resource}...", "READ", user_id, session["username"])

        rwLock = self.rwLocks[resource]
        fileActivity = self.fileActivity[resource]
        okay = rwLock.acquire_read_lock(user_id, timeout = 5) #Try to acquire lock, timeout after 5s

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

        if (resource and self.fileActivity[resource].is_reader(user_id)):    #Check if user has read lock or not, true = release
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

        if (resource and self.fileActivity[resource].is_writer(user_id)):    #Check if user has write lock or not, true = release
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
        self.capacity = n
        availability = max(0, n - self.activeUsers.count()) #Rebuild semaphore
        self.semaphore = threading.Semaphore(availability)
        self.log.add(
            f"Reconfigured: Semaphore N {oldCapacity} -> {n}. Available = {availability}", "SYS")
    
    def status(self):   #ReadOnly status of engine for connection to frontend API
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
            "semaphore": {
                "available": self.availability(),
                "capacity":       self.capacity,
                "occupied":  self.activeUsers.count(),
            },
            "rw":          selectedRw,
            "rwByResource": rw,
            "fileActivity": fileActivity,
            "resources":   list(self.resourceFiles.keys()),
            "files":       files,
            "file":        files["product.txt"],
            "log":        log,
            "info":       info,
        }

class RWLock:
    #Custom RW lock as Python has no built in RWLock
    def __init__(self):
        self.condition = threading.Condition(threading.Lock())
        self.readerCount = 0
        self.writerLock = threading.Lock()
        self.writerId = None
        self.readerIds = set()
        self.stateLock = threading.Lock()   #Protect writer id and reader ids

    def acquire_read_lock(self, user_id, timeout = 5):
        timer = time.time() + timeout   #Block for 5s if locked
        with self.condition:
            if user_id in self.readerIds:
                return False

            while (self.writerId is not None):
                remainingTime = timer - time.time()
                if (remainingTime <= 0):
                    return False
                self.condition.wait(timeout = remainingTime)

            #If no write lock, increase reader count and record user
            self.readerCount += 1
            self.readerIds.add(user_id)
            return True
        
    def release_read_lock(self, user_id):
        with self.condition:
            if user_id not in self.readerIds:
                return
            self.readerIds.discard(user_id)
            self.readerCount = max(0, self.readerCount - 1)
            #Wake waiting writer threads
            if (self.readerCount == 0):
                self.condition.notify_all()

    def acquire_write_lock(self, user_id, timeout = 5):
        if not (self.writerLock.acquire(timeout = timeout)):    #If can't acquire mutex, timeout after 5s
            return False
        timer = time.time() + timeout
        with self.condition:
            while (self.readerCount > 0):   #Wait for readers to stop
                remainingTime = timer - time.time()
                if (remainingTime <= 0):
                    self.writerLock.release()   #Release mutex
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

    
class CurrentSessions:
    def __init__(self):
        self.store = OrderedDict()    #Stores logged in users in order
        self.lock = threading.Lock()    #RW Lock
    
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
                self.store[user_id]["state"] = state    #Update the state field in dict
    
    def count(self):
        with self.lock:
            return len(self.store)
    
    def status(self):
        return list(self.store.values())

class UserAccessTracker:    #UI user monitor
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

    def clear_user(self, user_id):   #Use for user logout
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
            if (len(self.entries) > 300):   #Remove oldest entry if reached max
                self.entries.pop()

        if (category not in ("SYS",) and user_id is not None):
            thread = threading.Thread(
                target = database.write_audit,
                args = (user_id, username, category, message),
                daemon = True   #Daemon thread type for every call, no waiting for write
            )
            thread.start()

    def status(self, limit = 150):
        with self.lock:
            return list(self.entries[:limit])
    
    def clear(self):
        with self.lock:
            self.entries.clear()
