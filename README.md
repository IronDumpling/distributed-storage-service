# Introduction
This design document will describe how our client and server communicate and break it into several parts to introduce. It includes the client, communication logic of the client and server, and server storage disk. 
![1](https://github.com/IronDumpling/persistent-storage-server/assets/70104294/baaeeb07-438d-4397-8f7f-e99a79c4a019)

# Client
The client is implemented in the KVClient Class, which consists of two main components: the interactive UI for the Client Application and the execution of corresponding functionalities.

When the client application starts to run, it prints out the UI and continuously waits for the command input. The UI for the Client Application is a command-line prompt. We employ the handleCommand() function to distinguish inputs like connect, disconnect, put, get, etc. This function is also used to handle input errors such as invalid parameters and commands.

For each command, we have a dedicated function that handles errors specific to that command. These functions utilize the corresponding functions within the Communication Module Library to execute the intended functionality. Subsequently, based on the KVMessage returned by the Library functions, different information is relayed to the UI, allowing both the user and the developer to understand the execution status of the program.

# Communication Logic
## Message Protocol 
The message protocol is written in the KVMessage Class. This Class comprises the following information: statusType, stringType, key, value, and message. The Message within this Class is the information used during transmission.

To facilitate the construction of KVMessage in different scenarios, we have implemented three Constructors, each handling different inputs such as String and StatusType. All the data has been built in the constructor. 

The Message is what would be used in transmission. The format of the message is like this: <StatusType> + “ ” + <key> + “ ” + <value> + “\r\n”. If the Message does not have <value>, the format would be like this: <StatusType> + “ ” + <key> + “\r\n”.

During transmission, the Message undergoes UTF-8 Encoding, and during reception, it undergoes Decoding. These operations are implemented in KVMessageTool and fall under the SharedTool category.

## Client Communication Module
It is designed for communicating with the server from the client side. This includes functions such as disconnect(), connect(), put(), and get(). 
connect() and disconnect() are used to establish and close the corresponding socket for communication with the server.
When issuing a "put (key, value)" request to the server, the server performs a search in the cache first and then the storage disk to determine the existence of the specified key and doing the following operations:
- If the key is in the cache (not written to disk yet), the server updates the cache and waits for data to be written to the storage disk. And return PUT_UPDATE.
- If the key is not in the cache but exists in the storage disk, a new key-value pair is added to the cache. Duplicate pairs in the storage disk are ignored. And return PUT_UPDATE.
- If the key doesn't exist in either the cache or storage disk, the server stores it in the cache. 
- If key is not found in either the cache or storage disk, it will return PUT_SUCCESS 
- If the value is "null," indicating a delete request, the server follows a similar process as described above, returning DELETE_SUCCESS. The server responds with a GET_ERROR 
- If the key is not found. Upon locating the key in either the cache or storage disk, the server returns the corresponding value with a statusType of GET_SUCCESS.

## Server Communication Module
The Communication Module for the Server is implemented in KVServerCmnct. It is responsible for receiving KVMessage sent by the Client Socket and processing it accordingly. 

KVServerCmnct implements the Runnable interface, allowing it to run as a thread. For each new client joining, a new Server Communication Module is created to handle its communication. If any error happens or the socket connection has been disabled, the server socket will stop listening. 

The input would be handled by the recceiveMessage() function in the KVMessageTool(). Then it would call the corresponding function in the KVServer object to operate it. 

## Shared Tool
This section is coded in KVMessageTool and encompasses these tools: Two commonly used functions in the Communication Module, receiveMessage, and sendMessage, along with functions for parsing received strings. We implement these functions as shared tools because we want to standardize the process of receiving and sending messages so that we can handle errors in a single place. 
The ParseMessage function takes a String and parses it into a KVMessage based on its type. The receiveMessage function accepts an input stream, segments the information by "\r\n," converts it into a String, and then uses ParseMessage to interpret the String into a KVMessage. The sendMessage function receives an output stream, converts it into byte[], and then sends it out.

# Server
## Server Application
It will read arguments from the terminal to determine the port number, data storage path, server address, log path, and log level to initialize the server accordingly. The main job of the server application is to listen to the client’s connection request and create a new thread to communicate with the client. 

## Server Storage
We save the persistent data into a file called data0, it will be checked every time the server is launched and will be created if it does not exist.

data0 structure: every line is in a structure <line_number key value>, representing a KV pair. The line_number will be used in milestone 2 - see note below.

Put: We will first search the storage to determine the returning StatusType, if not an error status, we put the new KV pair to the last line of data0 as in-place updates cost too much.

Get: We will search from the end of data0, this is because we put new data at the end of data0, and the up-to-date data always appears at the end. 

Additional work for milestone 2: To prepare for the large storage size in milestone 2, we have created a metafile as well. We have also provided a function header compaction, this is to compact existing data and perform garbage collection. It is still a TODO for now.

## Server Cache 
We have implemented three cache strategies, FIFOCache, LRUCache, and LFUCache, all of the three classes extend a superclass Cache.

There is an inner class called CacheNode in the Cache class, it contains at least one attribute KVPair pair, which represents the actual key value pair stored in the cache. The three child Cache classes have a customized Node class, with some additional attributes other than the pair.

# Performance Report
In this test report, we will test different ratios of gets and puts to calculate the throughput and latency to measure the performance under different cache strategies as well. 
In this performance test, we organize requests into groups of 100 and systematically evaluate each group's performance under different operation distributions. Three cache replacement strategies – FIFO, LFU, and LRU – are employed for thorough analysis. Each group undergoes testing with specific ratios of put (write) and get (read) operations. The first group is subjected to an 80% put and 20% get distribution, the second group to an equal 50% put and 50% get distribution, and the third group to a reverse 80% get and 20% put distribution. The tests are conducted 50 times for each group, allowing us to collect sufficient data for statistical analysis. 

## Latency 
| Cache strategy | 80% puts + 20% gets | 50% puts + 50% gets | 20% puts + 80% gets |
| ---------|----------|----------|
| FIFO | 5469 ms | 4935 ms | 4550 ms |
| LFU | 5142 ms | 4785 ms | 4482 ms |
| LRU | 5409 ms | 4827 ms | 4557 ms | 

## Throughput 
| Cache strategy | 80% puts + 20% gets | 50% puts + 50% gets | 20% puts + 80% gets |
| ---------|----------|----------|
| FIFO | 18.2848 request/s | 20.2611 request/s | 21.9763 request/s |
| LFU | 19.4456 request/s | 20.8951 request/s | 22.3081 request/s |
| LRU | 18.4865 request/s | 20.7140 request/s | 21.9438 request/s | 

From the data we have collected, we can easily find out that a higher percentage of put operations corresponds to increased time consumption. We implement the read write lock in our system. Specifically, our system allows only one thread at a time to execute a put request. Consequently, when a larger proportion of requests involves puts, the sequential execution of these operations leads to a longer overall processing time. Although there is no significant difference between three different cache strategies, the implementation of the LFU (Least Frequently Used) cache strategy results in better performance compared to other strategies. 

