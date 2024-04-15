# Distributed-Storage-Service
# Introduction
We have established a distributed data storage structure with ECS as the control center and multiple equal servers as data stores. Users can freely add and remove servers. ECS will use the Consistent Hashing method to allocate the data stored on each server, ensuring that data is not lost and can be stored relatively evenly. The distributed system is based on the core method of Replication, utilizing eventual consistency. This system is designed to tolerate server crashes without data loss and support concurrent operations for a large number of servers and users.

<img width="1031" alt="截屏2024-04-15 12 13 02" src="https://github.com/IronDumpling/distributed-storage-service/assets/70104294/73afe028-5ac4-4e35-9e6c-577ebaf32276">

# ECS
ECS serves as the central control of the entire system and is responsible for managing all servers and the shared data metadata of servers. ECS is divided into the following components: the user input interface ECSClient, the server manager ECSCmnct, and the server communicator ECSNode.
## Launch
```
java -jar m4-ecs.jar -p <port>

java -jar m4-ecs.jar -p 40000
```
## Remove Servers
Remove the server gracefully from the ECS side.
```
ecsclient> remove_server <ServerIP:ServerPort>

ecsclient> remove_server 127.0.0.1:40001
```

# KVServer

## Launch
```
java -jar m4-server.jar -d <data_storage_path> -b <ECSIP:ECSPort> -p <port>

java -jar m4-server.jar -d ./data -b 127.0.0.1:40000 -p 40001
```
## Remove Servers
Crash the server directly by interrupting it.
```
CTRL + C
```

# KVClient
## Launch
```
java -jar m4-client.jar -p <port>

java -jar m4-client.jar -p 30000
```
## Put/Get
```
kvclient> put <key> <value> // put or update key

kvclient> put <key> null // delete key

kvclient> get <key>
```

## Subscribe/Unsubscribe
- A single record from the table

```
kvclient> subscribe <key>

kvclient> subscribe apple // subscribe key

kvclient> subscribe banana // subscribe key
```

- A list of key

Introduce a new API to list all subscribes:

```
kvclient> subscribe

Current Subscribers:
1 - apple
2 - banana
```

Introduce a new API to unsubscribe:

```
kvclient> unsubscribe <key>

kvclient> unsubscribe apple // subscribe key
```

The client opens a new thread to receive messages constantly. 

When the coordinator receives a PUT request, broadcast the updated values to all subscribed clients

## Non-Relational Table
```
kvclient> create_table <TableName> <field1 field2 field3 …>

kvclient> create_table CAR NAME PRICE

kvclient> put_table <TableName> <field1_value field2_value field3_value … >

kvclient> put_table CAR XIAOMI 200000

kvclient> select (*) FROM CAR

kvclient> select (NAME) FROM CAR 

kvclient> select (*,WHERE:PRICE>10000) FROM CAR

kvclient> select (NAME,PRICE,WHERE:PRICE<10000) FROM CAR
```

# Performance evaluation
We have designed two experiments:
- One client sees the performance for puts and gets under different numbers of servers
- One server sees the performance for puts and gets under different numbers of clients

## Running the tests
We have written sh scripts to run the performance test 

- For experiment 1: Please run sh p1.sh to get the output data, the script runs 5 experiments with different numbers of servers. A client will put and get 1000 key-value pairs from the distributed system formed by the servers.
To see the plot, run python3 plot1.py after running the output-generating script.

- For experiment 2: Please run sh p2.sh to get the output data, the script runs 5 experiments with different numbers of clients. These clients are running concurrently at different background processes, where each client will put and get 1000 key-value pairs from the server.
To see the plot, run python3 plot2.py after running the output-generating script.

## Experiment 1 Results
Note that: The time on the y-axis is the time taken (latency) for 1000 operations

Experiment 1 Milestone 3 Result:
<img width="1012" alt="截屏2024-04-15 12 05 04" src="https://github.com/IronDumpling/distributed-storage-service/assets/70104294/3056325e-3fe3-4ad9-a769-58636a6c6edf">

Experiment 1 Milestone 2 Result:
<img width="1014" alt="截屏2024-04-15 12 04 52" src="https://github.com/IronDumpling/distributed-storage-service/assets/70104294/6b6c6712-d481-4dc8-90f1-ed3923a9ef94">

Experiment 1 discussion:
We have optimized our locking strategies for milestone 3. Note that in milestone 2 we tested 1000 puts and gets, while in milestone 3, 1000 puts and gets run too fast (showing 0 seconds on gets), therefore, we expanded the testing set to 10000 puts and gets. The performance of puts becomes 10 times faster, while the performance of gets becomes 100 times faster.

## Experiment 2 Results
Experiment 2 Milestone 3 Result:
<img width="722" alt="截屏2024-04-15 11 58 33" src="https://github.com/IronDumpling/distributed-storage-service/assets/70104294/483547da-716d-4948-aec5-935b5b809f7f">

Experiment 2 Milestone 2 Result:
<img width="723" alt="截屏2024-04-15 11 59 06" src="https://github.com/IronDumpling/distributed-storage-service/assets/70104294/93a408c2-6fec-4164-adb8-50e7300a922f">

Experiment 2 discussion:
We used the same dataset (1000 puts and gets) as milestone 2 in experiment 2. Both the puts and gets benefit from our lock optimization. 
Both speeds now show a linear increase as the number of clients increases, since while the client number increases, they are in the race condition of write locks.




