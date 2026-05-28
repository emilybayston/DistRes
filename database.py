#Database storage connection for the server-owned credentials database

import sqlite3
import os
import threading


dbPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "consres.db")

writeLock = threading.Lock()

def connection():
    #Creates one SQLite connection for application-layer access functions
    con = sqlite3.connect(dbPath, check_same_thread = False)
    con.row_factory = sqlite3.Row
    #Allows readers to continue while another thread writes an audit or user update
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA foreign_keys = ON")
    return con
