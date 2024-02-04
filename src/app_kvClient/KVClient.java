package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import cmnct_client.KVCommInterface;
import cmnct_client.KVStore;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.KVMessageTool;

import shared.messages.IKVMessage.StatusType;

import constants.Constants;

public class KVClient implements IKVClient {

    private boolean stop = false;

    private BufferedReader stdin;
    private static Logger logger = Logger.getRootLogger();

    private KVStore kvStore;

    private String serverAddress;
	private int serverPort;

    /* Entry Point */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.ALL);
			KVClient client = new KVClient();
			client.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

    /* Client Side Application */
    public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(Constants.PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated", e);
			}
		}
    }

    private void handleCommand(String cmdLine) throws IOException{
        String[] tokens = cmdLine.split("\\s+", 3);

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
                    default:
                        commandError(tokens[0]);
                }
                break;
            case 2:
                switch (tokens[0].toLowerCase()){
                    case "logLevel":
                        logLevel(tokens[1]);
                        break;
                    case "get":
                        get(tokens[1]);
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

    private void help(){
        StringBuilder sb = new StringBuilder();
		sb.append("\nKV-CLIENT HELP (Usage):\n");
		sb.append("\n================================");
        sb.append("================================\n");
        
		sb.append("\nconnect <host> <port>");
		sb.append("\t\t Establishes a TCP connection to a server\n");

		sb.append("disconnect");
		sb.append("\t\t\t Disconnects from the server \n");
        
        sb.append("put <key> <value>");
        sb.append("\t\t Inserts a key-value pair into the storage server data structures.\n");
        sb.append("\t\t\t\t Updates (overwrites) the current value with the given value if the server already contains the specified key.\n");
        sb.append("\t\t\t\t Deletes the entry for the given key if <value> equals null.\n");

        sb.append("get <key>");
		sb.append("\t\t\t Retrieves the value for the given key from the storage server.\n");

		sb.append("logLevel");
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
        printSuccess("Application exit!");
    }

    private void logLevel(String level){
        boolean success = true;
        switch (level.toUpperCase()) {
            case "ALL":
                logger.setLevel(Level.ALL);
                break;
            case "DEBUG":
                logger.setLevel(Level.DEBUG);
                break;
            case "INFO":
                logger.setLevel(Level.INFO);
                break;
            case "WARN":
                logger.setLevel(Level.WARN);
                break;
            case "ERROR":
                logger.setLevel(Level.ERROR);
                break;
            case "FATAL":
                logger.setLevel(Level.FATAL);
                break;
            case "OFF":
                logger.setLevel(Level.OFF);
                break;
            default:
                printError("Unknown log level!", null);
                printPossibleLogLevels();
                success = false;
        }
        if(success) printSuccess("Current log level is: " + 
                                logger.getLevel().toString());
    }

    private void printPossibleLogLevels() {
		System.out.println("\t  Possible log levels are:");
		System.out.println("\t  ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

    private void commandError(String cmd){
        switch (cmd.toLowerCase()){
            case "":
                break;
            case "connect":
                printError("Invalid number of parameters!", null);
                break;
            case "disconnect":
                printError("Too many parameters!", null);
                break;
            case "put":
                printError("Invalid number of parameters!", null);
                break;
            case "get":
                printError("Invalid number of parameters!", null);
                break;
            case "logLevel":
                printError("Invalid number of parameters!", null);
                break;
            case "help":
                printError("Too many parameters!", null);
                break;
            case "quit":
                printError("Too many parameters!", null);
                break;
            default:
                printError("Unknown command!", null);
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
            printError("No valid port number. Port must be a number!", nfe);
        } catch (Exception e){
            printError("Unexpected things happen when tries to connect!", e);
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
            printError("No server connection!", e);
        } catch(Exception e){
            printError("Unexpected things happen when tries to disconnect the server", e);
        }
    }

    private void put(String key, String value){
        if(key.length() >= Constants.KEY_SIZE){
            printError("Key is too big (larger than " + Constants.KEY_SIZE + "Bytes), can't fill in!", null);
            return;
        }
        if(value.length() >= Constants.VAL_SIZE * 1024){
            printError("Value is too big (larger than " + Constants.VAL_SIZE + "KBytes), can't fill in!", null);
            return;
        }
        if(key.trim().isEmpty()){
            printError("Key can not be empty", null);
            return;
        }

        String key_value = key + ": " + value;

        try{
            IKVMessage msg = kvStore.put(key, value);
            StatusType status = msg.getStatus();
            switch (status) {
                case PUT_SUCCESS:
                    printSuccess("Put " + key_value + " SUCCESS!");
                    break;
                case PUT_UPDATE:
                    printSuccess("Put " + key_value + " UPDATE!");
                    break;
                case PUT_ERROR:
                    printError("Put " + key_value + " FAIL!", null);
                    break;
                case DELETE_ERROR:
                    printError(key_value + " DELETE FAIL!", null);
                    break;
                case DELETE_SUCCESS:
                    printSuccess(key_value + " DELETE SUCCESS!");
                    break;
                default:
                    printError("fail in wrong return status: " + status + " !", null);
            }
        } catch(NullPointerException e){ 
            printError("No server connection!", e);
        } catch(IOException e){
            printError("Unable to put " + key_value + " !", e);
            disconnection();
        } catch(Exception e){
            printError("Unexpected things happen when tries to put " + key_value + " !", e);
            disconnection();
        }
    }

    private void get(String key){
        if(key.length() >= Constants.KEY_SIZE){
            printError("Key is too big (larger than " + Constants.KEY_SIZE + "Bytes), can't fill in!", null);
            return;
        }
        if(key.trim().isEmpty()){
            printError("Key can not be empty", null);
            return;
        }

        try{
            IKVMessage msg = kvStore.get(key);
            StatusType status = msg.getStatus();
            switch (status) {
                case GET_SUCCESS:
                    printSuccess("Get " + key + ": " + msg.getValue());
                    break;
                case GET_ERROR:
                    printError("Can't get "+ key + "!", null);
                    break;
                default:
                    printError("Fail in wrong return status: " + status + " !", null);
                    break;
            }
        } catch(NullPointerException e){
            printError("No server connection!", e);
        } catch(IOException e){
            printError("Unable to get " + key + " !", e);
            disconnection();
        } catch(Exception e){
            printError("Unexpected things happen when tries to get " + key + " !", e);
            disconnection();
        }
    }

    /* General Helpers */
    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }

    public void printError(String msg, Exception e){
        System.out.println(Constants.PROMPT + "[ERROR]: " + msg);
        if(e != null) logger.error(msg, e);
    }
    
    public void printSuccess(String msg){
        System.out.println(Constants.PROMPT + "[SUCCESS]: " + msg);
        logger.info(msg);
	}

    public void printInfo(String msg){
        System.out.println(Constants.PROMPT + msg);
        logger.info(msg);
    }
}
