# Distributed-Storage-Service
# Introduction
We have established a distributed data storage structure with ECS as the control center and multiple equal servers as data stores. Users can freely add and remove servers. ECS will use the Consistent Hashing method to allocate the data stored on each server, ensuring that data is not lost and can be stored relatively evenly.
# ECS
ECS serves as the central control of the entire system and is responsible for managing all servers and the shared data metadata of servers. ECS is divided into the following components: the user input interface ECSClient, the server manager ECSCmnct, and the server communicator ECSNode.
```
java -jar m4-ecs.jar -p <port>
```

# KVServer
```
java -jar m4-server.jar -d ./data -b <ECSaddress:ECSPort> -p <port>
```

# KVClient
```
java -jar m4-client.jar -p <port>
```
## Put/Get

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

# Test report


# Performance Evaluation

However, we can also see that the latency difference is very small, it might be because the write is quite efficient compared to sending and receiving messages, and the lock is not creating too much overhead.


