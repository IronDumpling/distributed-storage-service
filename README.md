# Introduction
This design document will describe how our client and server communicate and break it into several parts to introduce. It includes client, communication logic of client and server and server storage disk. 
![1](https://github.com/IronDumpling/persistent-storage-server/assets/70104294/baaeeb07-438d-4397-8f7f-e99a79c4a019)

# Client
The client is implemented in the KVClient Class, which consists of two main components: the interactive UI for the Client Application and the execution of corresponding functionalities.

When the client application starts to run, it prints out the UI and continuously waits for the command input. The UI for the Client Application is a command-line prompt. We employ the handleCommand() function to distinguish inputs like connect, disconnect, put, get, etc. This function is also used to handle input errors such as invalid parameters and commands.

For each command, we have a dedicated function that handles errors specific to that command. These functions utilize the corresponding functions within the Communication Module Library to execute the intended functionality. 

Subsequently, based on the KVMessage returned by the Library functions, different information is relayed to the UI, allowing both the user and the developer to understand the execution status of the program.

We also implemented helper functions such as printError(), printSuccess() to make the process of output and logging more standardize and easier. 
# Communication Logic
## Message Protocol 
The message protocol is written in the KVMessage Class. This Class comprises the following information: statusType, stringType, key, value, and message. The Message within this Class is the information used during transmission.

To facilitate the construction of KVMessage in different scenarios, we have implemented three Constructors, each handling different inputs such as String and StatusType. All the data has been built in the constructor. 

The Message is what would be used in transmission. The format of the message is like this: <StatusType> + “ ” + <key> + “ ” + <value> + “\r\n”. If the Message does not have <value>, the format would be like this: <StatusType> + “ ” + <key> + “\r\n”.

During transmission, the Message undergoes UTF-8 Encoding, and during reception, it undergoes Decoding. These operations are implemented in KVMessageTool and fall under the SharedTool category.
## Client Communication Module
It is designed for communicating with the server from the client side. This includes functions such as disconnect(), connect(), put(), and get(). 

connect() and disconnect() are used to establish and close the corresponding socket for communication with the server.

When issuing a "put (key, value)" request to the server, the server performs a search in the cache first and then the storage disk to determine the existence of the specified key. If the key is found in the cache, indicating it hasn't been written to the disk yet, the server updates the corresponding key-value pair in the cache and waits data to be written to the storage disk. If the key is not in the cache but exists in the storage disk, a new key-value pair is added to the cache. Notably, any duplicate key-value pairs in the storage disk are disregarded during this process, as they don't impact our get search algorithm, which searches from the newest to the oldest data. Handling of duplicate pairs is only considered when additional space is needed for storage, a topic to be addressed in the storage section. The server then returns PUT_UPDATE. In the scenario where the key doesn't exist in either the cache or storage disk, the server stores it in the cache. The data is written to the storage disk when the buffer is full, and the server returns PUT_SUCCESS. If any exceptions occur on the server side, a PUT_ERROR status is returned.

If the value is "null," indicating a delete request, the server follows a similar process as described above, returning DELETE_SUCCESS. The only distinction lies in cases where there's no existence of the key-value pair or an exception occurs on the server side, in which the server returns DELETE_ERROR.

When a "get key" request is sent to the server, it systematically searches both the cache and storage disk. The server responds with a GET_ERROR if the key is not found. Upon locating the key in either the cache or storage disk, the server returns the corresponding value with a statusType of GET_SUCCESS.
## Server Communication Module
The Communication Module for the Server is implemented in KVServerCmnct. It is responsible for receiving KVMessage sent by the Client Socket and processing it accordingly. 

KVServerCmnct implements the Runnable interface, allowing it to run as a thread. For each new client joining, a new Server Communication Module is created to handle its communication. If any error happens or the socket connection has been disabled, the server socket will stop listening. 

The input would be handled by the recceiveMessage() function in the KVMessageTool(). Then it would call the corresponding function in the KVServer object to operate it. 
## Shared Tool
This section is coded in KVMessageTool and encompasses these tools: Two commonly used functions in the Communication Module, receiveMessage, and sendMessage, along with functions for parsing received strings.
We implement these functions as shared tools because we want to standardize the process of receiving and sending messages so that we can handle errors in a single place. 
The ParseMessage function takes a String and parses it into a KVMessage based on its type. 
The receiveMessage function accepts an input stream, segments the information by "\r\n," converts it into a String, and then uses ParseMessage to interpret the String into a KVMessage.
The sendMessage function receives an output stream, converts it into byte[], and then sends it out.
# Server
## Server Application
It will read arguments from the terminal to determine the port number, data storage path, server address, log path, and log level to initialize the server accordingly. 
The main job of the server application is to listen to the client’s connection request and create a new thread to communicate with the client. 
## Server Storage

## Server Cache 

