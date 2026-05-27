#Main concurrency engine

import threading
import queue
import time
from datetime import datetime
from collections import OrderedDict

import database

#Main engine class - ConRes core functionality
class ConRes:
    def __init__(self, capacity = 4):
        self.capacity = capacity
        self.semaphore = threading.Semaphore(capacity)  #Counting semaphore, initial capacity at 4
        self.rwLock = RWLock()

        self.activeUsers = CurrentSessions()    #Logged in users
        self.waitingUsers = queue.Queue(maxsize = 0)    #FIF queue for threads waiting for semaphore
        self.fileActivity = UserAccessTracker()
        self.log = EventLog()

        self.file = self.defaultFile()
        self.lock = threading.Lock()    #File content write lock

        self.info = dict(logins = 0, reads = 0, writes = 0, blocked = 0)
        self.infoLock = threading.Lock()

        self.threads = 1    #Thread counter
        
    def defaultFile(self):
        return (
            "ProductSpecification.txt" + '\n'
            "Concurrency and  Communication Module" + '\n'
            f"Last updated: {datetime.now():%Y-%m-%d %H:%M:%S}"
        )
    
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

        if (self.fileActivity.is_reader(user_id)):    #Auto release locks
            self.rwLock.release_read_lock(user_id)
            self.fileActivity.remove_reader(user_id)
            self.log.add(f"Released: {user_id} read lock released.", "READ", user_id, session["username"])
            

        if self.fileActivity.is_writer(user_id):
            self.rwLock.release_write_lock(user_id)
            self.fileActivity.clear_writer(user_id)
            self.log.add(f"Released: {user_id} write lock released.", "WRITE", user_id, session["username"])

        self.semaphore.release()    #Release to wake waiting threads
        self.log.add(
            f"Logout: {user_id} ({session['username']}) - "
            f"Thread {session['threadId']} terminated. Semaphore released.",
            "OUT", user_id, session["username"]
        )
        return {"okay": True}

    def acquire_read_lock(self, user_id):
        session = self.activeUsers.get(user_id)  #Check user logged in
        if (not session):
            return {"okay": False, "error": "User not logged in."}

        self.log.add(f"Read request: {user_id} acquiring shared read lock...", "READ", user_id, session["username"])

        okay = self.rwLock.acquire_read_lock(user_id, timeout = 5) #Try to acquire lock, timeout after 5s

        if (okay):
            self.activeUsers.update_state(user_id, "READING")
            self.fileActivity.add_reader(user_id)
            self.info_increment("reads")
            state = self.rwLock.status()
            self.log.add(
                f"Read lock: {user_id} - lock acquired. "
                f"{len(state['readers'])} concurrent reader(s).",
                "READ", user_id, session["username"]
            )
        else:
            
            self.log.add(
                f"Read blocked: {user_id} - writer {self.rwLock.writerId} "
                f"holds lock. Timeout 5s. Deadlock avoidance.",
                "WARN", user_id, session["username"]
            )
        return {"okay": okay, "error": "" if okay else "Read blocked - writer active (5s timeout)."}
    
    def acquire_write_lock(self, user_id):
        session = self.activeUsers.get(user_id)
        if (not session):
            return {"okay": False, "error": "User not logged in."}

        self.log.add(f"Write Request: {user_id} acquiring exclusive write lock...", "WRITE", user_id, session["username"])

        okay = self.rwLock.acquire_write_lock(user_id, timeout = 5)

        if (okay):
            self.activeUsers.update_state(user_id, "WRITING")
            self.fileActivity.set_writer(user_id)
            self.info_increment("writes")
            self.log.add(
                f"Write lock: {user_id} - exclusive lock. All other readers/writers blocked.",
                "WRITE", user_id, session["username"]
            )
        else:
            state = self.rwLock.status()
            reason = (f"writer {state['writer']} holds lock" if state["writer"]
                      else f"{state['reader_count']} reader(s) active")
            self.log.add(
                f"Write blocked: {user_id} - {reason}. "
                f"Consistent lock order enforced. Deadlock prevented.",
                "WARN", user_id, session["username"]
            )
        return {"okay": okay, "error": "" if okay else "Write blocked - resource busy (5s timeout)."}
    
    def release(self, user_id):
        session = self.activeUsers.get(user_id)
        released = False

        if (self.fileActivity.is_reader(user_id)):    #Check if user has read lock or not, true = release
            self.rwLock.release_read_lock(user_id)
            self.fileActivity.remove_reader(user_id)
            self.activeUsers.update_state(user_id, "IDLE")
            state = self.rwLock.status()
            self.log.add(
                f"Read lock released: {user_id}. {state['reader_count']} reader(s) remain.",
                "READ", user_id, session["username"] if session else None
            )
            released = True

        if (self.fileActivity.is_writer(user_id)):    #Check if user has write lock or not, true = release
            self.rwLock.release_write_lock(user_id)
            self.fileActivity.clear_writer(user_id)
            self.activeUsers.update_state(user_id, "IDLE")
            self.log.add(
                f"Write lock released: {user_id} - resource now available.",
                "WRITE", user_id, session["username"] if session else None
            )
            released = True

        if (not released):
            return {"okay": False, "error": "No lock held by this user."}
        return {"okay": True}

    def commit_write(self, user_id, newContent):
        session = self.activeUsers.get(user_id)

        if( not self.fileActivity.is_writer(user_id)):
            return {"ok": False, "error": "No write lock held."}

        with self.lock:
            self.file = newContent

        self.log.add(
            f"Write commit: {user_id} saved changes.",
            "WRITE", user_id, session["username"] if session else None
        )
        return {"ok": True}
    
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
        rw = self.rwLock.status()
        fileActivity = self.fileActivity.status()
        log = self.log.status(100)

        with self.lock:
            content = self.file

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
            "rw":          rw,
            "fileActivity": fileActivity,
            "file":        content,
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
            while (self.writerId is not None):
                remainingTime = timer - time.time()
                if (remainingTime <= 0):
                    return False
                self.condition.wait(timeout = remainingTime)

            #If no write lock, increase reader count and record user
            self.readerCount += 1
            with self.stateLock:
                self.readerIds.add(user_id)
            return True
        
    def release_read_lock(self, user_id):
        with self.condition:
            self.readerCount = max(0, self.readerCount - 1)
            with self.stateLock:
                self.readerIds.discard(user_id)
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
        with self.stateLock:
            if (self.writerId != user_id):
                return False
            self.writerId = None

        with self.condition:
            self.condition.notify_all()
            try:
                self.writerLock.release()
            except RuntimeError:
                pass
            return True
        
    def status(self):
        with self.stateLock:
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