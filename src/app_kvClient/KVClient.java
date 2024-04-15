package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import cmnct_client.KVCommInterface;
import cmnct_client.KVStore;
import cmnct_client.KVClientSSocket;
import cmnct_server.KVServerSSocket;
import shared.Constants;
import shared.KVUtils;
import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage.StatusType;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Random;
import shared.Constants;


public class KVClient implements IKVClient {

    private boolean stop = false;

    private BufferedReader stdin;
    private static Logger logger = Logger.getRootLogger();

    private KVStore kvStore;

    private String serverAddress;
	private int serverPort;

    private String socketInfo;
    private int port;
    private ServerSocket serverSocket;
    private HashMap<Integer, Socket> serverSockets = new HashMap<>();
    private List<Socket> connectedSockets = new ArrayList<>();
    private List<String> subscribeList = new ArrayList<>();

    /* Entry Point */
    public static void main(String[] args) {
        boolean invalidInput = false;
        boolean hasPort = false;
        int port = 0;

    	try {
			new LogSetup("logs/client.log", Level.ALL);

            for(int i = 0; i < args.length; i++){
                switch (args[i]) {
                    case "-p":
                        if (i + 1 < args.length) {
                            port = Integer.parseInt(args[i + 1]);
                            hasPort = true;
                            i++;  
                        } else {
                            KVUtils.printError("Missing value for -p");
                            invalidInput = true;
                        }
                        break;
                    default:
                        KVUtils.printError("Unknown option: " + args[i]);
                        invalidInput = true;
                }
            }

            if(invalidInput) {
                KVUtils.printError("Invalid number of arguments!");
                KVUtils.printInfo("Usage: Client -p <port number> !", logger);
            } else if(!hasPort) {
                KVUtils.printError("Missing Port!");
                KVUtils.printInfo("Usage: Client -p <port number> !", logger);
            } else {
		    	KVClient client = new KVClient(port);
		    	client.run();
            }
		} catch (IOException ioe) {
            KVUtils.printError("Unable to initialize logger!", ioe, logger);
			System.exit(1);
		} catch (NumberFormatException nfe) {
            KVUtils.printError("Invalid argument! Not a number!", nfe, logger);
            System.exit(1);
        }
    }

    /* Client Side Application */
    public KVClient(int port){
        KVUtils.printInfo("Initialize client ...");
        
        try {
            this.serverSocket = new ServerSocket(port);
            KVUtils.printSuccess("Client listening on port: " + serverSocket.getLocalPort(), logger);
        } catch(IOException ioe) {
            KVUtils.printError("Unexpected things happen when initialize client's subscription socket!", ioe, logger);
        }

        this.port = port;
        this.socketInfo = serverSocket.getInetAddress().getHostAddress().toString() + ":" + serverSocket.getLocalPort();
    }


    public void run() {        
        Thread serverThread = new Thread(() -> {
            serverSocket();
        });
        serverThread.start();

		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(Constants.PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				KVUtils.printError("CLI does not respond - Application terminated", e, logger);
			}
		}
    }

    public void serverSocket() {
        while(!stop){
            try {
                Socket client = serverSocket.accept(); 
                KVClientSSocket connection = new KVClientSSocket(client, this);
                connectedSockets.add(client);
                new Thread(connection).start();
                KVUtils.printInfo("Connected to " + client.getInetAddress().getHostName() +  
                                " on port " + client.getPort(), logger);
            } catch (IOException ioe) {
                KVUtils.printError("Unable to establish connection!", ioe, logger);
            }
        }
    }

    private void handleCommand(String cmdLine) throws IOException{
        String[] tokens = cmdLine.split("\\s+", 2);
        String query;
        String token;
        if(tokens[0].toLowerCase().compareTo("select") == 0){
            query = tokens[1];
            Pattern pattern = Pattern.compile("\\w+|\\([^)]*\\)");
            Matcher matcher = pattern.matcher(query); 
            int matchCount = 0;
            String selectFields = null;
            String selectTable= null;
            String selectCondition= null;
            if(matcher.find()){
                selectFields = matcher.group().trim();
                selectFields = selectFields.substring(1,selectFields.length()-1);
            }
            if(matcher.find()){
                if(matcher.group().trim().toLowerCase().compareTo("from") != 0){
                    commandError(tokens[0]);
                    return;
                }
            }
            if(matcher.find()){
                selectTable = matcher.group().trim();
            }
            select(selectFields,selectTable);
            
        }else if(tokens[0].toLowerCase().compareTo("create_table") == 0 ){
            tokens = tokens[1].split("\\s+",2);
            if(tokens.length == 2){
                create_table(tokens[0], tokens[1]);
            }else{
                commandError("create_table");
            }

        }else if(tokens[0].toLowerCase().compareTo("put_table") == 0 ){
            tokens = tokens[1].split("\\s+",2);
            if(tokens.length == 2){
                put_table(tokens[0], tokens[1]);
            }else{
                commandError("put_table");
            }
        }else{
            tokens = cmdLine.split("\\s+", 4);
            switch (tokens.length){
                case 1:
                    switch (tokens[0].toLowerCase()){
                        case "quit":
                            quit();
                            break;
                        case "help":
                            help();
                            break;
                        case "disconnect":
                            disconnection();
                            break;
                        case "subscribe":
                            subscribe(); // list all subscribe
                            break;
                        case "keyrange":
                            keyrange();
                            break;
                        default:
                            commandError(tokens[0]);
                    }
                    break;
                case 2:
                    switch (tokens[0].toLowerCase()){
                        case "loglevel":
                            logger.setLevel(KVUtils.getLogLevel(tokens[1]));
                            break;
                        case "get":
                            get(tokens[1]);
                            break;
                        case "subscribe":
                            subscribe(tokens[1]);
                            break;
                        case "unsubscribe":
                            unsubscribe(tokens[1]);
                            break;
                        case "destroy_table":
                            destroy_table(tokens[1]);
                            break;
                        default:
                            commandError(tokens[0]);
                    }
                    break;
                case 3:
                    switch (tokens[0].toLowerCase()){
                        case "connect":
                            connection(tokens[1], tokens[2]);
                            break;
                        case "put":
                            put(tokens[1], tokens[2]);
                            break;
                        default:
                            commandError(tokens[0]);
                    }
                    break;
                default:
                    commandError(tokens[0]);
            }
        }
    }

    private void help(){
        StringBuilder sb = new StringBuilder();
		sb.append("\nKV-CLIENT HELP (Usage):\n");
		sb.append("\n================================");
        sb.append("================================\n");
        
		sb.append("\nconnect <host> <port>");
		sb.append("\t\t Establishes a TCP connection to a server\n");

		sb.append("disconnect");
		sb.append("\t\t\t Disconnects from the server \n");

        sb.append("\n");
        
        sb.append("put <key> <value>");
        sb.append("\t\t Inserts a key-value pair into the storage server data structures.\n");
        sb.append("\t\t\t\t Updates (overwrites) the current value with the given value if the server already contains the specified key.\n");
        sb.append("\t\t\t\t Deletes the entry for the given key if <value> equals null.\n");

        sb.append("get <key>");
		sb.append("\t\t\t Retrieves the value for the given key from the storage server.\n");

        sb.append("keyrange");
        sb.append("\t\t\t Retrieves the keyrange of connected server.\n");

        sb.append("\n");

        sb.append("subscribe");
        sb.append("\t\t\t No parameter means return the list of subscribes.\n");
        
        sb.append("subscribe <key>");
        sb.append("\t\t\t Subscribe a key, when the pair is changed, it would return the value.\n");

        sb.append("unsubscribe <key>");
        sb.append("\t\t Unsubscribe one of the subscribed key.\n");

        sb.append("\n");
        
        sb.append("create_table <name> <fields>");
        sb.append("\t Create a table. Default table is 'KV'\n");

        sb.append("put_table <table> <key> <value>");
        sb.append("\t Put value to the table.\n");

        sb.append("destroy_table <table>");
        sb.append("\t\t Destroy a table, clear all data in the table.\n");

        sb.append("\n");

		sb.append("loglevel");
		sb.append("\t\t\t Sets the logger to the specified log level. \n");
		sb.append("\t\t\t\t ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF\n");
		
		sb.append("quit");
        sb.append("\t\t\t\t Exits the program. \n");

        sb.append("\n================================");
        sb.append("================================\n");
        
		System.out.println(sb.toString());
    }

    private void quit(){
        stop = true;
        disconnection();
        KVUtils.printSuccess("Application exit!", logger);
    }

    private void commandError(String cmd){
        switch (cmd.toLowerCase()){
            case "":
                break;
            case "connect":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "disconnect":
                KVUtils.printError("Too many parameters!");
                break;
            case "keyrange":
                KVUtils.printError("Too many parameters!");
                break;
            case "put":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "get":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "loglevel":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "help":
                KVUtils.printError("Too many parameters!");
                break;
            case "quit":
                KVUtils.printError("Too many parameters!");
                break;
            case "subscribe":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "put_table":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "create_table":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "destroy_table":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "unsubscribe":
                KVUtils.printError("Invalid number of parameters!");
                break;
            case "select":
                KVUtils.printError("Invalid number of parameters!");
                break;
            default:
                KVUtils.printError("Unknown command!");
                help();
        }
    }

    /* Communication Libarary Usage */
    private void connection(String hostname, String port){
        try{
            serverAddress = hostname;
            serverPort = Integer.parseInt(port);
            newConnection(serverAddress, serverPort);
        } catch (NumberFormatException nfe) {
            KVUtils.printError("No valid port number. Port must be a number!", nfe, logger);
        } catch (Exception e){
            KVUtils.printError("Unexpected things happen when tries to connect!", e, logger);
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
    }

    private void disconnection(){
        try{
            kvStore.disconnect();
            kvStore = null;
        } catch(NullPointerException e){
            KVUtils.printError("No server connection!", e, logger);
        } catch(Exception e){
            KVUtils.printError("Unexpected things happen when tries to disconnect the server", e, logger);
        }
    }

    private void put(String key, String value){
        if(key.length() >= Constants.KEY_SIZE){
            KVUtils.printError("Key is too big (larger than " + Constants.KEY_SIZE + "Bytes), can't fill in!");
            return;
        }
        if(value.length() >= Constants.VAL_SIZE * 1024){
            KVUtils.printError("Value is too big (larger than " + Constants.VAL_SIZE + "KBytes), can't fill in!");
            return;
        }
        if(key.trim().isEmpty()){
            KVUtils.printError("Key can not be empty");
            return;
        }

        String key_value = key + ": " + value;

        try{
            IKVMessage msg = kvStore.put(key, value);
            if (msg == null){
                KVUtils.printInfo("Unable to put " + key_value + " !");
            }else{
                StatusType status = msg.getStatus();
                switch (status) {
                    case PUT_SUCCESS:
                        KVUtils.printSuccess("Put " + key_value + " SUCCESS!", logger);
                        break;
                    case PUT_UPDATE:
                        KVUtils.printSuccess("Put " + key_value + " UPDATE!", logger);
                        break;
                    case PUT_ERROR:
                        KVUtils.printError("Put " + key_value + " FAIL!");
                        break;
                    case DELETE_ERROR:
                        KVUtils.printError(key_value + " DELETE FAIL!");
                        break;
                    case DELETE_SUCCESS:
                        KVUtils.printSuccess(key_value + " DELETE SUCCESS!", logger);
                        break;
                    case SERVER_STOPPED:
                        KVUtils.printError(key_value + " SERVER STOPPED!");
                        break;
                    case SERVER_WRITE_LOCK:
                        KVUtils.printError(key_value + " SERVER WRITE LOCK!");
                        break;
                    default:
                        KVUtils.printError("fail in wrong return status: " + status + " !");
                }
            }
            
        } catch(NullPointerException e){ 
            KVUtils.printError("No server connection!", e, logger);
        } catch(IOException e){
            KVUtils.printError("Unable to put " + key_value + " !", e, logger);
            disconnection();
        } catch(Exception e){
            KVUtils.printError("Unexpected things happen when tries to put " + key_value + " !", e, logger);
            disconnection();
        }
    }

    private void get(String key){           
        if(key.length() >= Constants.KEY_SIZE){
            KVUtils.printError("Key is too big (larger than " + Constants.KEY_SIZE + "Bytes), can't fill in!", null, logger);
            return;
        }
        if(key.trim().isEmpty()){
            KVUtils.printError("Key can not be empty", null, logger);
            return;
        }

        try{
            IKVMessage msg = kvStore.get(key);
            if(msg == null){
                KVUtils.printInfo("Unable to get " + key + " !");
            }else{
                StatusType status = msg.getStatus();
                switch (status) {
                    case GET_SUCCESS:
                        KVUtils.printSuccess("Get " + key + ": " + msg.getValue(), logger);
                        break;
                    case GET_ERROR:
                        KVUtils.printError("Can't get "+ key + "!");
                        break;
                    case SERVER_STOPPED:
                        KVUtils.printError("SERVER STOPPED!");
                        break;
                    default:
                        KVUtils.printError("Fail in wrong return status: " + status + " !");
                        break;
                }
            }
        } catch(NullPointerException e){
            KVUtils.printError("No server connection!", e, logger);
        } catch(IOException e){
            KVUtils.printError("Unable to get " + key + " !", e, logger);
            disconnection();
        } catch(Exception e){
            KVUtils.printError("Unexpected things happen when tries to get " + key + " !", e, logger);
            disconnection();
        }
    }

    private void keyrange(){
        try{
            IKVMessage msg = kvStore.keyrange();
            StatusType status = msg.getStatus();
            switch (status) {
                case META_UPDATE:
                    KVUtils.printSuccess("get keyrange: " + msg.getMessage(), logger);
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError(" SERVER STOPPED!");
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !");
                    break;
            }
        } catch(NullPointerException e){
            KVUtils.printError("No server connection!", e, logger);
        } catch(IOException e){
            KVUtils.printError("Unable to get new metaData!", e, logger);
            disconnection();
        } catch(Exception e){
            KVUtils.printError("Unexpected things happen when tries to get!", e, logger);
            disconnection();
        }
    }

    public void subscribe(){
        KVUtils.printInfo("Current SubScribes:");
        int index = 1;
        for (String item : subscribeList) {
            KVUtils.printInfo(index + " - " + item);
            index ++;
        }
    }

    public void subscribe(String key){
        IKVMessage msg = kvStore.subscribe(key, socketInfo);
        if (msg == null){
            KVUtils.printInfo("Unable to subscribe " + key + "!");
        }else{
            StatusType status = msg.getStatus();
            switch (status) {
                case SUBSCRIBE_SUCCESS:
                    subscribeList.add(key);
                    KVUtils.printSuccess("Subscribe " + key + " successfully!");
                    break;
                case SUBSCRIBE_FAIL:
                    KVUtils.printError("Can't subscribe "+ key + "!");
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError("SERVER STOPPED!");
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !");
                    break;
            } 
        }
    }

    // public void subscribe(String table, String condition){

    // }

    public void unsubscribe(String key){
        boolean keyIsFind = false;

        for (String item : subscribeList) {
            if(item.compareTo(key)==0){
                keyIsFind = true;
            }
        }
        
        if(!keyIsFind){
            KVUtils.printInfo("key: " + key + " doesn't exist!");
            return;
        }

        IKVMessage msg = kvStore.unsubscribe(key, socketInfo);
        
        if (msg == null){
            KVUtils.printInfo("Unable to subscribe " + key + " !");
        }else{
            StatusType status = msg.getStatus();
            switch (status) {
                case UNSUBSCRIBE_SUCCESS:
                    subscribeList.remove(key);
                    KVUtils.printSuccess("Unsubscribe " + key + " successfully!");
                    break;
                case UNSUBSCRIBE_FAIL:
                    KVUtils.printError("Can't unsubscribe "+ key + "!");
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError("SERVER STOPPED!");
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !");
                    break;
            } 
        }
    }

    public void create_table(String tableName, String tableFields){
        IKVMessage msg = kvStore.create_table(tableName,tableFields);
        if (msg == null){
            KVUtils.printInfo("Unable to create table!");
        }else{
            StatusType status = msg.getStatus();
            switch (status) {
                case CREATE_TABLE_SUCCESS:
                    KVUtils.printSuccess("Create Table:"+ tableName + " with Fields: "+ tableFields +" successfully!");
                    break;
                case CREATE_TABLE_FAIL:
                    KVUtils.printError("Unable to create Table:"+tableName+" with Fields: "+ tableFields +"!");
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError("SERVER STOPPED!");
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !");
                    break;
            } 
        }
    }
    
    public void put_table(String table, String fields){
        IKVMessage msg = kvStore.put_table(table,fields);
        if (msg == null){
            KVUtils.printInfo("Unable to put " + fields + " in " + table + "!");
        }else{
            StatusType status = msg.getStatus();
            switch (status) {
                case PUT_TABLE_SUCCESS:
                    KVUtils.printSuccess("put " + fields + " in " + table + "successfully!");
                    break;
                case PUT_TABLE_FAIL:
                    KVUtils.printError("Uanble to put " + fields + " in " + table + "!");
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError("SERVER STOPPED!");
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !");
                    break;
            } 
        }
    }

    public void destroy_table(String tableName){
        IKVMessage msg = kvStore.destroy_table(tableName);
        if (msg == null){
            KVUtils.printInfo("Unable to destory table: "+ tableName+"!");
        }else{
            StatusType status = msg.getStatus();
            switch (status) {
                case DESTROY_TABLE_SUCCESS:
                    KVUtils.printSuccess("Destory Table: "+tableName+" successfully!");
                    break;
                case DESTROY_TABLE_FAIL:
                    KVUtils.printError("Unable to destory Table: "+tableName+" successfully!");
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError("SERVER STOPPED!");
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !");
                    break;
            } 
        }
    }

    public void select(String fields, String table){
        String[] selectResult =null;
        selectResult = kvStore.select(table, fields);
        if (selectResult == null){
            KVUtils.printInfo("Unable to select Table: "+table +" with Fields: "+fields+"!");
        }else{
            //TODO: print it out
            //print selectResult
            KVUtils.printInfo("Select result of Table: " + table);
            KVUtils.printInfo(fields.replace(",", " "));
            for(String attribute:selectResult){
                KVUtils.printInfo(attribute);
            }
            
        }
    }


    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }

}
