package cmnct_client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import java.net.ServerSocket;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.KVClient;

import shared.KVPair;
import shared.KVMeta;
import shared.KVUtils;
import shared.Constants.ServerStatus;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage.StatusType;

public class KVClientSSocket implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private boolean isRunning;
        
    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    private String serverName;
    private String clientName;

    private KVClient kvClient;

    public KVClientSSocket(Socket clSocket, KVClient kvClient){
        this.clientSocket = clSocket;
        this.isRunning = true;
        this.kvClient = kvClient;
    }

    @Override
    public void run(){
        try {
            connect();
            operation();
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
            isRunning = false;
        } finally {
            try {
                input.close();
                output.close();
                clientSocket.close();
            } catch (NullPointerException e) {
                logger.error("Client socket has lost!", e);
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    public void connect() throws IOException {
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();
        IKVMessage msg = new KVMessage(StatusType.INFO, "Connection to KV client established: ");
        KVMessageTool.sendMessage(msg, output);
        System.out.println("KVClient connected to the server!");
    }

    public void operation() throws IOException {
        while(isRunning){
            try {
                IKVMessage recMsg = KVMessageTool.receiveMessage(input);
                if (recMsg == null){
                    isRunning = false;
                    return;
                } 

                IKVMessage sendMsg;

                if(recMsg.getStatus() == StatusType.SUBSCRIBE_EVENT){
                    if(recMsg.getValue().compareTo("null") == 0){
                        KVUtils.printInfo("\n========================================");
                        KVUtils.printInfo("Subscribed key " + recMsg.getKey() + " has been deleted");
                        KVUtils.printInfo("========================================\n");
                    }
                    else{
                        KVUtils.printInfo("\n========================================");
                        KVUtils.printInfo("Subscribed key " + recMsg.getKey() + " has been updated to " + recMsg.getValue());
                        KVUtils.printInfo("========================================\n");
                    }
                }
                else{
					handleFail();
				}
            } catch (IOException ioe) {
                logger.error("Error! Connection lost!", ioe);
                isRunning = false;
            }
        }
    }

    /* Message Handler */
    private IKVMessage handleFail() {
        IKVMessage message = new KVMessage(StatusType.FAILED, 
                                "Unknown Request Message Type!");
        logger.info("Unknown Request Message Type!");
        return message;
    }
    
}
