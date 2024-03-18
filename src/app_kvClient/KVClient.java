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

import shared.Constants;
import shared.KVUtils;
import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage.StatusType;

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
		} catch (IOException ioe) {
            KVUtils.printError("Unable to initialize logger!", ioe, logger);
			ioe.printStackTrace();
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
				KVUtils.printError("CLI does not respond - Application terminated", e, logger);
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

        sb.append("keyrange");
        sb.append("\t\t\t Retrieves the keyrange of connected server.\n");

		sb.append("");
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
                KVUtils.printError("Invalid number of parameters!", null, logger);
                break;
            case "disconnect":
                KVUtils.printError("Too many parameters!", null, logger);
                break;
            case "keyrange":
                KVUtils.printError("Too many parameters!", null, logger);
                break;
            case "put":
                KVUtils.printError("Invalid number of parameters!", null, logger);
                break;
            case "get":
                KVUtils.printError("Invalid number of parameters!", null, logger);
                break;
            case "loglevel":
                KVUtils.printError("Invalid number of parameters!", null, logger);
                break;
            case "help":
                KVUtils.printError("Too many parameters!", null, logger);
                break;
            case "quit":
                KVUtils.printError("Too many parameters!", null, logger);
                break;
            default:
                KVUtils.printError("Unknown command!", null, logger);
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
            KVUtils.printError("Key is too big (larger than " + Constants.KEY_SIZE + "Bytes), can't fill in!", null, logger);
            return;
        }
        if(value.length() >= Constants.VAL_SIZE * 1024){
            KVUtils.printError("Value is too big (larger than " + Constants.VAL_SIZE + "KBytes), can't fill in!", null, logger);
            return;
        }
        if(key.trim().isEmpty()){
            KVUtils.printError("Key can not be empty", null, logger);
            return;
        }

        String key_value = key + ": " + value;

        try{
            IKVMessage msg = kvStore.put(key, value);
            StatusType status = msg.getStatus();
            switch (status) {
                case PUT_SUCCESS:
                    KVUtils.printSuccess("Put " + key_value + " SUCCESS!", logger);
                    break;
                case PUT_UPDATE:
                    KVUtils.printSuccess("Put " + key_value + " UPDATE!", logger);
                    break;
                case PUT_ERROR:
                    KVUtils.printError("Put " + key_value + " FAIL!", null, logger);
                    break;
                case DELETE_ERROR:
                    KVUtils.printError(key_value + " DELETE FAIL!", null, logger);
                    break;
                case DELETE_SUCCESS:
                    KVUtils.printSuccess(key_value + " DELETE SUCCESS!", logger);
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError(key_value + " SERVER STOPPED!", null, logger);
                    break;
                case SERVER_WRITE_LOCK:
                    KVUtils.printError(key_value + " SERVER WRITE LOCK!", null, logger);
                    break;
                default:
                    KVUtils.printError("fail in wrong return status: " + status + " !", null, logger);
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
            StatusType status = msg.getStatus();
            switch (status) {
                case GET_SUCCESS:
                    KVUtils.printSuccess("Get " + key + ": " + msg.getValue(), logger);
                    break;
                case GET_ERROR:
                    KVUtils.printError("Can't get "+ key + "!", null, logger);
                    break;
                case SERVER_STOPPED:
                    KVUtils.printError(" SERVER STOPPED!", null, logger);
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !", null, logger);
                    break;
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
                    KVUtils.printError(" SERVER STOPPED!", null, logger);
                    break;
                default:
                    KVUtils.printError("Fail in wrong return status: " + status + " !", null, logger);
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

    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }
}
