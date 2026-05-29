# DistRes
DistRes is a distributed client-server system designed to coordinate access to shared resources across multiple nodes. It extends the functionality of the Concurrent Resource Access and Synchronisation Engine (ConRes) by introducing distributed communication, fault tolerance, and synchronised multi-node coordination.

The system allows multiple clients to concurrently read shared resources while ensuring that only one client can perform write operations at a time. DistRes uses distributed communication mechanisms, layered architecture principles, and synchronisation techniques to maintian consistency and prevent race conditions.

**Features**
- Distributed client-server communication
- Concurrent multi-client resource access
- Read-write synchronisation mechanisms
- Publish-subscribe notification system
- Shared database and file access
- Fault toleance with retries and reconnections
- Layered software architecture
- Prevention of race conditions and data inconsistency


**System Architecture**


DistRes Component Diagram:


<img width="482" height="244" alt="image" src="https://github.com/user-attachments/assets/ff6a9783-c197-406c-a0b2-e945066fe068" />


DistRes Deployment Diagram:


<img width="451" height="333" alt="image" src="https://github.com/user-attachments/assets/6326a240-7232-49a5-a897-b5477d60c691" />




Client Nodes:
  - Connect to the distributed server
  - Send resource access requests
  - Read and write to shared resources
  - Receive update notifications from the server
 
Server Node:
  - Hosts the shared resources
  - Manages user credential storage
  - Coordinattes client requests
  - Enforces synchronisation policies
  - Handles publish-subscribe notifications


**Shared Resources**

User Credential Database:
  - Username
  - Password

Shared Distributed Files:
  - ProductSpecification.txt
  - TeamNotes.txt


**Resource Synchronisation**

DistRes supports:
  - Multiple concurrent readers
  - Single writer access
  - Read-write locks for coordination
  - Consistency enforcement across distibuted nodes

This prevents:
  - Race conditions
  - Data corruption
  - Simultaneous confliciting writes


**Publish-Subscribe Mechanism**

When a resource is updated:
  1. The server publishes an update event
  2. All subscribed active clients receive notifications
  3. Clients synchronise their local states

This ensures distributed consistency and real-time coordination.


**Fault Tolerance**

The system includes:
  - Automatic reconnection attempts
  - Retry mechanisms for failed requests
  - Handling of node/server failures

These features improve reliability in distributed environments.


**Technologies and Concepts**

- Distributed systems
- Socket programming/RPC
- Concurrency and multithreading
- Read-write locks
- Publish-subscribe pattern
- Fault tolerance
- Layered software design


**Example Workflow**

1. Client connects to server
2. Client authenticates using credentials
3. Client requests resource access
4. Server validates synchronisation rules
5. Resource is read or updated
6. Update notifications are published to active clients


 **Running the Program**

1. Run server.py

<img width="731" height="110" alt="image" src="https://github.com/user-attachments/assets/cb6ceb30-96d8-47f0-a778-df730c2785fb" />


2. Run api.py

<img width="725" height="172" alt="image" src="https://github.com/user-attachments/assets/7152c493-4bf3-4e3d-befa-5996a99f123c" />


3. Open the browser client ip address (http://127.0.0.1:5000)

<img width="1165" height="849" alt="image" src="https://github.com/user-attachments/assets/9930371c-0574-40b0-80f5-f99370713933" />




