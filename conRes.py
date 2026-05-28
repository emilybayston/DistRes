#Main coordination engine for DistRes
#This is where the ConRes ideas are kept: sessions, semaphores, queues and locks
#The socket server calls this file when a client wants to login, read, write or commit
#The data layer does the actual file/database work after this class allows it

import threading
import queue
import time
import os
from datetime import datetime
from collections import OrderedDict

import dataLayer

class DistRes:
    #Coordinates login capacity, active users and safe access to shared resource files

    #Builds the server-side coordination state used by all connected clients
    def __init__(self, capacity = 4):
        #This keeps the original ConRes coordination model as the core of DistRes
        self.capacity = capacity
        self.semaphoreControl = SemaphoreController(capacity)
        self.activeUsers = ConcurrentUserManagement()    #Stores logged in users, roles, session ids and activity state
        self.waitingUsers = WaitingUserQueue()    #Stores login sessions waiting for a free server capacity slot
        self.threadManager = ThreadManager()
        self.log = EventLog()
        #Shared file access is delegated to the data layer, while locks stay in the application layer
        self.fileData = dataLayer.SharedFileDataAccess(os.path.dirname(os.path.abspath(__file__)))
        self.resourceFiles = self.fileData.resourceFiles
        #Each resource has its own read-write controller so different files can be used at the same time
        self.rwLocks = {resource: ReadWriteSynchronizationController() for resource in self.resourceFiles}
        self.fileActivity = {resource: UserAccessTracker() for resource in self.resourceFiles}
        self.fileData.ensure_files()
        #Access control remembers which user owns which resource lock
        self.accessControl = AccessControlCoordinator()
        self.lock = threading.Lock()    #Serialises physical file writes so committed content is not interleaved

        self.info = dict(logins = 0, reads = 0, writes = 0, blocked = 0)
        self.infoLock = threading.Lock()
        
    #Returns default text for a shared file if it has to be created
    def defaultFile(self, resource):
        return self.fileData.default_file(resource)

    #Makes sure the shared resource files exist before clients can use them
    def ensure_resource_files(self):
        #Creates missing resources without putting file creation logic in the coordinator
        self.fileData.ensure_files()

    #Keeps resource names consistent even if the UI sends an unexpected value
    def normalise_resource(self, resource):
        #Normalising in one place stops invalid UI values from breaking lock dictionaries
        return self.fileData.normalise_resource(resource)

    #Reads a shared resource through the data layer
    def read_file(self, resource):
        return self.fileData.read_file(resource)

    #Writes a shared resource through the data layer
    def write_file(self, resource, content):
        self.fileData.write_file(resource, content)

    #Finds a locked resource for the dashboard summary
    def locked_resource(self):
        return self.accessControl.locked_resource()

    #Gets every resource currently held by one user
    def user_locked_resources(self, user_id):
        return self.accessControl.user_locked_resources(user_id)

    #Finds a user's first lock when the release request does not name a file
    def first_user_resource(self, user_id):
        return self.accessControl.first_user_resource(user_id)

    #Checks whether a user still has any active file lock
    def user_holds_lock(self, user_id):
        return self.accessControl.user_holds_lock(user_id)

    #Checks the lock mode a user owns for a specific resource
    def user_resource(self, user_id, resource):
        resource = self.normalise_resource(resource)
        return self.accessControl.user_resource(user_id, resource)

    #Reserves a resource for a user before trying to take the read-write lock
    def reserve_user_resource(self, user_id, resource, mode):
        #Reserves ownership before waiting on the RWLock so duplicate requests fail early
        #Allows one user to read different files while blocking repeated reads of the same file
        resource = self.normalise_resource(resource)
        return self.accessControl.reserve_user_resource(user_id, resource, mode)

    #Clears a user's resource ownership after release or logout
    def clear_user_resource(self, user_id, resource = None):
        return self.accessControl.clear_user_resource(user_id, resource)
    
    #Returns how many active session slots are still free
    def availability(self):
        return self.semaphoreControl.availability(self.activeUsers.count())
    
    #Updates simple counters used in the dashboard
    def info_increment(self, key):
        with self.infoLock:
            self.info[key] += 1

    #Reads the waiting queue without removing anyone from it
    def waiting_list_peek(self):
        return self.waitingUsers.items()
    
    #Authenticates a user and either admits them or places them in the queue
    def login(self, user_id, password):
        #Credentials are checked against the server database before any session is created
        user = dataLayer.userCredentialData.authenticate(user_id, password)
        if (not user):
            self.log.add(f"Authentication Failed: {user_id} - invalid user details.", "WARN", user_id)
            return {"okay": False, "error": "Invalid user ID or password."}
        id, name, role = user["user_id"], user["username"], user["role"]

        if (self.activeUsers.contains(id)):    #Prevents the same user id from opening two active sessions
            return {"okay": False, "error": f"{user_id} already logged in."}
        for user in self.waiting_list_peek():
            if (user["user_id"] == id):
                return {"okay": False, "error": f"{user_id} already in queue."}
            
        #A thread id is assigned so the dashboard can show the server-side session
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
    
    #Runs in the background for a queued user until a capacity slot opens
    def wait_entry(self, session):  #Waits for a released capacity slot and promotes the queued session
        #Queued users sleep here until the semaphore releases a server capacity slot
        self.semaphoreControl.wait()
        self.waitingUsers.remove(session["user_id"])

        self.allow(session, pushed=True)

    #Adds a user to the active session list once the semaphore allows it
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
    
    #Logs a user out and releases any resource locks they were still holding
    def logout(self, user_id):
        #Logout cleans up any resource locks before the session capacity is released
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

    #Gives a logged in user shared read access if no writer is active
    def acquire_read_lock(self, user_id, resource = "ProductSpecification.txt"):
        session = self.activeUsers.get(user_id)  #Rejects resource access unless the user has an active session
        if (not session):
            return {"okay": False, "error": "User not logged in."}
        resource = self.normalise_resource(resource)

        #Access control prevents the same user repeatedly taking the same read lock
        reserved, heldResource = self.reserve_user_resource(user_id, resource, "READ")
        if (not reserved):
            return {"okay": False, "error": f"{user_id} already holds a lock for {heldResource}."}

        self.log.add(f"Read request: {user_id} acquiring shared read lock for {resource}...", "READ", user_id, session["username"])

        rwLock = self.rwLocks[resource]
        fileActivity = self.fileActivity[resource]
        okay = rwLock.acquire_read_lock(user_id, timeout = 5) #Allows shared read access unless a writer holds the file

        if (okay):
            #Multiple readers can be active together, so the tracker stores every reader id
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
    
    #Gives a logged in user exclusive write access if the resource is free
    def acquire_write_lock(self, user_id, resource = "ProductSpecification.txt"):
        session = self.activeUsers.get(user_id)
        if (not session):
            return {"okay": False, "error": "User not logged in."}
        resource = self.normalise_resource(resource)

        #Write access is exclusive, so the user must release other locks before writing
        reserved, heldResource = self.reserve_user_resource(user_id, resource, "WRITE")
        if (not reserved):
            return {"okay": False, "error": f"{user_id} already holds a lock for {heldResource}. Release it first."}

        self.log.add(f"Write Request: {user_id} acquiring exclusive write lock for {resource}...", "WRITE", user_id, session["username"])

        rwLock = self.rwLocks[resource]
        fileActivity = self.fileActivity[resource]
        okay = rwLock.acquire_write_lock(user_id, timeout = 5)

        if (okay):
            #Only the recorded writer will later be allowed to commit file changes
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
    
    #Releases the selected read or write lock for this user
    def release(self, user_id, resource = None):
        #Release works for the selected file, or falls back to the first lock the user owns
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

    #Saves edited file content only if this user still owns the write lock
    def commit_write(self, user_id, newContent, resource = "ProductSpecification.txt"):
        #Commit is separate from write-lock acquisition so the UI can edit before saving
        session = self.activeUsers.get(user_id)
        resource = self.normalise_resource(resource)

        if(self.user_resource(user_id, resource) != "WRITE" or not self.fileActivity[resource].is_writer(user_id)):
            return {"okay": False, "error": "Only the user holding the write lock can commit this file."}

        #The data layer write is protected so two physical writes cannot overlap
        with self.lock:
            self.write_file(resource, newContent)

        self.log.add(
            f"Write commit: {user_id} saved changes to {resource}.",
            "WRITE", user_id, session["username"] if session else None
        )
        return {"okay": True, "resource": resource}
    
    #Changes the server session capacity from the dashboard slider
    def update_slots(self, n):
        oldCapacity = self.capacity
        availability = self.semaphoreControl.reconfigure(n, self.activeUsers.count())
        self.capacity = self.semaphoreControl.capacity
        self.log.add(
            f"Reconfigured: Semaphore N {oldCapacity} -> {n}. Available = {availability}", "SYS")
    
    #Builds one snapshot of the current system for the dashboard
    def status(self):   #Builds a read only snapshot for dashboard polling and lock visualisation
        #Status is read only and combines sessions, locks, files and event history
        activeUsers = self.activeUsers.status()
        waitingUsers = self.waiting_list_peek()
        rw = {resource: lock.status() for resource, lock in self.rwLocks.items()}
        for resource, state in rw.items():
            state["resource"] = resource
        selectedResource = self.locked_resource() or "ProductSpecification.txt"
        selectedRw = dict(rw[selectedResource])
        fileActivity = {resource: tracker.status() for resource, tracker in self.fileActivity.items()}
        log = self.log.status(100)

        with self.lock:
            files = self.fileData.read_all_files()

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
            "log":        log,
            "info":       info,
        }

class SemaphoreController:
    #Controls server session capacity and isolates semaphore operations from login logic

    #Creates the semaphore used to limit active users
    def __init__(self, capacity):
        #The semaphore limits how many users can be active on the server at once
        self.capacity = capacity
        self.semaphore = threading.Semaphore(capacity)
        self.lock = threading.Lock()

    #Calculates how many user slots are still available
    def availability(self, active_count):
        #Availability is calculated from active sessions so the dashboard stays accurate
        with self.lock:
            return max(0, self.capacity - active_count)

    #Tries to take a slot without making the login request wait
    def acquire_now(self):
        #Login uses a non-blocking acquire so full capacity can place the user in a queue
        return self.semaphore.acquire(blocking = False)

    #Blocks a queued login thread until a slot becomes free
    def wait(self):
        #Queued login threads block here until another user logs out
        self.semaphore.acquire()

    #Returns one session slot when a user logs out
    def release(self):
        self.semaphore.release()

    #Applies a new capacity value while keeping current active users in mind
    def reconfigure(self, capacity, active_count):
        #Reconfiguration rebuilds the semaphore around the new capacity limit
        with self.lock:
            self.capacity = capacity
            available = max(0, capacity - active_count)
            self.semaphore = threading.Semaphore(available)
            return available

    #Returns the capacity values shown in the sidebar
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

    #Creates the counter used for readable session thread names
    def __init__(self):
        #A counter keeps thread labels simple and readable in the interface
        self.counter = 1
        self.lock = threading.Lock()

    #Returns the next label shown for a user session
    def next_session_id(self):
        with self.lock:
            thread_id = f"Thread{self.counter}"
            self.counter += 1
            return thread_id

    #Starts a background thread for a queued user
    def start_waiter(self, target, session, user_id):
        #The waiter thread lets the UI return immediately while the user waits for capacity
        thread = threading.Thread(
            target = target,
            args = (session,),
            daemon = True,
            name = f"DistResWaiter{user_id}"
        )
        thread.start()
        return thread


class WaitingUserQueue:
    #Wraps the FIFO queue so waiting user operations stay outside the main coordinator

    #Creates an empty FIFO waiting queue
    def __init__(self):
        #FIFO order keeps admission fair when the server capacity is full
        self.queue = queue.Queue(maxsize = 0)

    #Adds a user to the end of the waiting queue
    def put(self, session):
        self.queue.put(session)
        return self.queue.qsize()

    #Returns a safe snapshot of waiting users for the dashboard
    def items(self):
        #The dashboard needs a snapshot without removing users from the queue
        with self.queue.mutex:
            return list(self.queue.queue)

    #Removes a user from the queue once they are promoted
    def remove(self, user_id):
        #Promotion removes a queued user by id after their semaphore wait finishes
        with self.queue.mutex:
            items = [s for s in self.queue.queue if s["user_id"] != user_id]
            self.queue.queue.clear()
            self.queue.queue.extend(items)
            self.queue.not_empty.notify_all()
            self.queue.not_full.notify_all()


class AccessControlCoordinator:
    #Tracks which resource locks each active user owns

    #Creates the ownership map used to stop duplicate or conflicting locks
    def __init__(self):
        self.userResources = {}
        self.lock = threading.Lock()

    #Returns one locked resource when the dashboard needs a quick summary
    def locked_resource(self):
        #Returns one currently locked resource for the dashboard summary
        with self.lock:
            for locks in self.userResources.values():
                for resource in locks:
                    return resource
            return None

    #Returns all locks owned by one user
    def user_locked_resources(self, user_id):
        #Returns all resources currently owned by one user
        with self.lock:
            return dict(self.userResources.get(user_id, {}))

    #Chooses one lock to release if the request does not specify a resource
    def first_user_resource(self, user_id):
        #Finds a default resource to release when no selected file is supplied
        with self.lock:
            for resource in self.userResources.get(user_id, {}):
                return resource
            return None

    #Checks whether a user owns any resource lock
    def user_holds_lock(self, user_id):
        #Checks whether the user still owns any read or write lock
        with self.lock:
            return bool(self.userResources.get(user_id))

    #Returns READ, WRITE or None for one user and resource
    def user_resource(self, user_id, resource):
        #Returns the lock mode a user owns for one resource
        with self.lock:
            return self.userResources.get(user_id, {}).get(resource)

    #Records intended ownership before the lock wait begins
    def reserve_user_resource(self, user_id, resource, mode):
        #Blocks duplicate same-file locks while allowing reads on different files
        with self.lock:
            locks = self.userResources.setdefault(user_id, {})
            if resource in locks:
                return False, resource
            if mode == "WRITE" and locks:
                return False, ", ".join(locks.keys())
            if mode == "READ" and "WRITE" in locks.values():
                return False, ", ".join(locks.keys())
            locks[resource] = mode
            return True, resource

    #Deletes ownership once a lock has been released
    def clear_user_resource(self, user_id, resource = None):
        #Removes ownership when a lock is released or a user logs out
        with self.lock:
            if resource is None:
                return self.userResources.pop(user_id, None)
            locks = self.userResources.get(user_id)
            if not locks:
                return None
            removed = locks.pop(resource, None)
            if not locks:
                self.userResources.pop(user_id, None)
            return removed


class RWLock:
    #Allows multiple concurrent readers or one exclusive writer for a single resource

    #Creates the counters and locks used to coordinate readers and writers
    def __init__(self):
        #The condition coordinates readers waiting for writers and writers waiting for readers
        self.condition = threading.Condition(threading.Lock())
        self.readerCount = 0
        #The writer gate prevents two writers entering the write path together
        self.writerLock = threading.Lock()
        self.writerId = None
        self.readerIds = set()
        self.stateLock = threading.Lock()   #Protects writer and reader identifiers returned to the dashboard

    #Lets a user read unless a writer currently owns the resource
    def acquire_read_lock(self, user_id, timeout = 5):
        timer = time.time() + timeout   #Limits how long a read request waits behind an active writer
        with self.condition:
            #A user cannot take the same read lock twice
            if user_id in self.readerIds:
                return False

            while (self.writerId is not None):
                #Readers wait only while an exclusive writer owns the resource
                remainingTime = timer - time.time()
                if (remainingTime <= 0):
                    return False
                self.condition.wait(timeout = remainingTime)

            #Records this user as a current reader after confirming no writer is active
            self.readerCount += 1
            self.readerIds.add(user_id)
            return True
        
    #Removes a reader and wakes waiting writers if the file is now free
    def release_read_lock(self, user_id):
        with self.condition:
            if user_id not in self.readerIds:
                return
            self.readerIds.discard(user_id)
            self.readerCount = max(0, self.readerCount - 1)
            #Wakes waiting writers only when the final reader leaves the resource
            if (self.readerCount == 0):
                self.condition.notify_all()

    #Lets one writer in only after all readers have left
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
            #The writer id becomes the authority used later during commit validation
            self.writerId = user_id
            return True
        
    #Clears the writer and wakes clients waiting for the resource
    def release_write_lock(self, user_id):
        with self.condition:
            #Only the user who owns the write lock can release it
            if (self.writerId != user_id):
                return False
            self.writerId = None
            self.condition.notify_all()
            try:
                self.writerLock.release()
            except RuntimeError:
                pass
            return True
        
    #Returns the lock state in a format the dashboard can display
    def status(self):
        with self.condition:
            return {
                "readerCount": self.readerCount,
                "readers": list(self.readerIds),
                "writer": self.writerId,
            }

    
ReadWriteSynchronizationController = RWLock


class UserManagement:
    #Thread-safe active user and session store used by login, logout and dashboard polling

    #Creates the active user store used by the server session list
    def __init__(self):
        self.store = OrderedDict()    #Stores logged in users in session order for stable UI display
        self.lock = threading.Lock()    #Protects the session store during add, remove and state updates
    
    #Adds a new active session after login is allowed
    def add(self, user_id, session):
        #A successful login creates one active session record
        with self.lock:
            self.store[user_id] = session
    
    #Removes the active session when a user logs out
    def remove(self, user_id):
        #Logout removes the active session before capacity is released
        with self.lock:
            return self.store.pop(user_id)
    
    #Finds a user session if that user is active
    def get(self, user_id):
        with self.lock:
            return self.store.get(user_id)
        
    #Checks whether a user id is already active
    def contains(self, user_id):
        with self.lock:
            return user_id in self.store
    
    #Updates the user's state shown in the dashboard
    def update_state(self, user_id, state):
        with self.lock:
            if (user_id in self.store):
                self.store[user_id]["state"] = state    #Updates the user's activity state shown in the dashboard
    
    #Returns the number of currently active sessions
    def count(self):
        with self.lock:
            return len(self.store)
    
    #Returns all active sessions for the dashboard
    def status(self):
        #The UI reads this list to draw the active session panels
        return list(self.store.values())


CurrentSessions = UserManagement
ConcurrentUserManagement = UserManagement

class UserAccessTracker:    #Tracks current readers and writer for one resource
    #Creates reader and writer tracking for one resource
    def __init__(self):
        #This mirrors the lock state in a dashboard friendly format
        self.lock = threading.Lock()
        self.readerIds = set()
        self.writerId = None

    #Adds a user to the visible reader list
    def add_reader(self, user_id):
        #Reader ids are stored as a set so duplicates cannot appear in the UI
        with self.lock:
            self.readerIds.add(user_id)
    
    #Removes a user from the visible reader list
    def remove_reader(self, user_id):
        with self.lock:
            self.readerIds.discard(user_id)
    
    #Stores the user currently writing this resource
    def set_writer(self, user_id):
        #Only one writer id is stored because write access is exclusive
        with self.lock:
            self.writerId = user_id
    
    #Clears the writer if the same user releases the write lock
    def clear_writer(self, user_id):
        with self.lock:
            if (self.writerId == user_id):
                self.writerId = None
    
    #Checks whether the user is currently reading this resource
    def is_reader(self, user_id):
        with self.lock:
            return user_id in self.readerIds
    
    #Checks whether the user is currently writing this resource
    def is_writer(self, user_id):
        with self.lock:
            return self.writerId == user_id
        
    #Returns reader and writer details for the dashboard
    def status(self):
        with self.lock:
            return {
                "readerCount": len(self.readerIds),
                "readers": list(self.readerIds),
                "writer": self.writerId, 
            }

    #Removes a user from both reader and writer tracking during cleanup
    def clear_user(self, user_id):   #Removes a user from reader and writer tracking during cleanup
        with self.lock:
            self.readerIds.discard(user_id)
            if (self.writerId == user_id):
                self.writerId = None

class EventLog():
    #Creates the in-memory event list used by the dashboard
    def __init__(self):
        #The in-memory log gives immediate feedback while SQLite keeps the audit trail
        self.entries = []
        self.lock = threading.Lock()
    
    #Adds a visible event and optionally writes it to the audit database
    def add(self, message, category = "INFO", user_id = None, username = None):
        #Every visible event gets a timestamp before being shown in the dashboard
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
            #Audit writing is backgrounded so logging does not slow down resource actions
            thread = threading.Thread(
                target = dataLayer.userCredentialData.write_audit,
                args = (user_id, username, category, message),
                daemon = True   #Writes audit entries in the background without delaying user actions
            )
            thread.start()

    #Returns recent events for the dashboard log
    def status(self, limit = 150):
        with self.lock:
            return list(self.entries[:limit])
    
    #Clears the visible in-memory log
    def clear(self):
        with self.lock:
            self.entries.clear()
