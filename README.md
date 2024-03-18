# Distributed-Storage-Service
# Introduction
We have established a distributed data storage structure with ECS as the control center and multiple equal servers as data stores. Users can freely add and remove servers. ECS will use the Consistent Hashing method to allocate the data stored on each server, ensuring that data is not lost and can be stored relatively evenly.
# ECS
ECS serves as the central control of the entire system and is responsible for managing all servers and the shared data metadata of servers. ECS is divided into the following components: the user input interface ECSClient, the server manager ECSCmnct, and the server communicator ECSNode.
## ECSClient
ECSClient is used to receive user commands and pass them to the server manager to execute corresponding tasks. Users can indicate specific servers to remove or add by entering the server's IP and port. This allows servers that are already running to leave or enter the distributed system we designed. Additionally, users can enter "print" to view the current system status and information.
## ECSCmnct & ECSNode
ECSCmnct is a thread created by ECSClient, which has a ServerSocket that continuously listens for new connection requests. Whenever a new connection is established, ECSCmnct creates a new thread to run ECSNode.
ECSNode first responds to the server attempting to establish a connection, then receives the server's port. It then continuously listens for received messages and updates the server's status.
ECSCmnct manages two pieces of shared data: metadata and an array of ECSNodes. A new ECSNode is created and added to the array whenever a new connection is made. Then, ECSCmnct uses a shared Consistent Hash function to calculate the hash value and generate new metadata. ECSNode then establishes a Client Socket connection with the server's ServerSocket and broadcasts the generated metadata to all servers.
When ECSClient receives the "add server" command, ECSCmnct attempts to establish a connection with the server's ServerSocket via a temporary ClientSocket to prompt it to join the system.
When ECSClient receives the "remote server" command, ECSCmnct locates the corresponding ECSNode for the server and removes it from the array. Then, ECSCmnct regenerates the metadata and broadcasts the generated metadata to all servers, including the removed server.
# KVServer
Based on the milestone1, we will face the situation of multiple KVServers, which needs to communicate with each other for data transfer besides communicating with Client. 
We add “client” socket (distinguished from server socket) to communicate with other KVServers and ECS Client. 
When the KVServer starts, the Server will establish a socket connection to the ECS and ECS will also establish the socket connection to the KVServer server socket to send the meta-data and following meta-data updates through this connection. 
To notify ECS Client about the current server status, we will send current server status when it changes through the socket connection. This socket connection will be terminated when the server status is marked as REMOVE when ECS Client decides to remove it.
The server status includes four types: IN_TRANSFER, STOPPED, ACTIVATED.
STOPPED: It is equivalent to SERVER_STOPPED. During initialization, before KVServer actually start, the server status is STOPPED. Another situation is KVServer is removed by ECS Client. In our design, we use meta-data to check if the current size should be removed by checking if it contains this address and port. And it doesn’t equal to shut down the KVServer. It will update its current status to STOPPED and reject all the communication from the client or server. 
ACTIVATED: When initialization is finished or after IN_TRANSFER, the current server status will change to ACTIVATED. It represents that it will accept and process new communications normally.
IN_TRANSFER: When KVServer receive the new meta-data update, KVServer will invoke the data transfer function to check and modify the storage database if it needs changes.  During this process, server will mark itself as IN_TRANSFER status, which is equivalent to SERVER_WRITE_LOCK. It will send key value as pair to the scheduled place through the “client” socket connection according to the meta-data.  

We have redesigned our storage server. During data transfer, we will loop over existing files, and create two categories of files: Data that will stay in the server and data to be transferred.
After transferring the data, we will remove the old files, as well as the data copy to be transferred.
# KVClient
KVClient will do the same thing just like milestone 1 except addition storage of meta-data and one more command “keyrange”. Since now, we have a distributed storage on the KVServers, stored data might not on the server the client connects to. Hence, we need meta-data which stores the information to help me determine which server we should connect to retrieve the desired data.  
Client will try to add, modify or delete the value to the server database. Client will send the message containing put request to the connected server first. If it succeeds, it means that our operation is completed. If not, it means that the serve is not in ACTIVATED. The client will keep sending the message until the message is processed by the server. If it is not on this server, serve will respond with SERVER_NOT_RESPONSIBLE. Then, client will ask serve to send its meta-data to update client side meta-data. Then, according to the new meta-data, it will connect to that serve to fetch the data. 
Same thing happens for GET. The only difference is that it will get respond when serve is under SERVER_WRITE_LOCK. 
keyrange is to illustrate the meta-data stored on the client side. In our design, it will ask connected serve to provide the client the newest meta-data and print it out in the terminal. 

All above operations include the automation of reconnecting to the other available KVServe according to the meta-data when the connected KVServe is removed or shut down. 

# Test report
In m2 test, we are focusing on the new features added compared with milestone 1. 
In our performance test, we have done several things list below:
Since our design is ECS Client controls the ADD and REMOVE of the KVSever, client might find the KVSever connected to is removed by the ECS CLient. Hence, client must handle the connection lost case, which will redirect itself to connect to other available KVSever. And our code pass the test.
ECS need to keep track of the number and server status change of the current change and update the meta-data accordingly. Test passed when we create 3 KVSevers one by one, meta-data is update and broadcast
Test on GET X, X representing key exit in the other KVSever, not the connected one  and successfully return the key by redirect to the correct KVSever.
Test on PUT X Y. X is the new key which doesn’t exit in the KVSever. It stored successfully in the storage place in the KVSever.
Test on PUT X Y, where X exists in the KVSever. It successfully redirects itself to connect to the KVSever which store that key and modify the key. 
Test on keyrange. Successfully return the newest keyrange when add and delete KVSever through the ECS Client.
Test on Data transfer for 100 stored key-value pair. ECS Client compute new meta-data and KVSevers all transfer the data correctly according to the new meta-data when add and remove the KVSever.
Test on the multiple KVSever. We have test 50 KVSever running at the same and ECS can control all of them and calculate the correct meta-data
Test on unexpected shutdown of the server. ECS Client can catch the connection lost with shutdown server and update the meta-data.
Server should perform corresponding response when the server is in write_lock and the status changes correctly and ECS can catch the server status. 

# Performance Evaluation
We have designed two experiments
One client, see the performance for puts and gets under different number of servers
One server, see the performance for puts and gets under different number of clients

Running the tests
We have written sh scripts to run the performance tests 

For experiment 1: 
Please run sh p1.sh to get the output data, the script runs 5 experiments with different numbers of servers. A client will put and get 1000 key value pairs from the distributed system formed by the servers.
To see the plot, run python3 plot1.py after running the output generating script.

For experiment 2:
Please run sh p2.sh to get the output data, the script runs 5 experiments with different numbers of clients. These clients are running concurrently at different background processes, where each client will put and get 1000 key value pairs from the server.
To see the plot, run python3 plot2.py after running the output generating script.


Below are the results for the two experiments
Note that: The time on the y axis is the time taken (latency) for 1000 operations

Experiment 1 Result:

Experiment 1 discussion:
We can see that as the number of servers increases, the time for both gets and puts increases, this is because the client needs to frequently connect and disconnect from the server, which creates significant overhead.
Experiment 2 Result:

Experiment 2 discussion:
For puts, we can see that there is a jump for latency from single client to multiple clients, this is because the write lock started to block other clients

Generally, for puts and gets, the latency increases as the number of clients increases. This is because of the write lock. 

However, we can also see that the latency difference is very small, it might be because the write is quite efficient compared to sending and receiving messages, and the lock is not creating too much overhead.


