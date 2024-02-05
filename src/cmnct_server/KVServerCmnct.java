package cmnct_server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import java.net.ServerSocket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage.StatusType;

public class KVServerCmnct implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private boolean isRunning;
        
    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    private String serverName;
    private String clientName;

    private KVServer server;

    public KVServerCmnct(Socket clSocket, KVServer kvServer){
        this.clientSocket = clSocket;
        this.isRunning = true;
        this.serverName = kvServer.getHostname() + 
                        " / " + kvServer.getPort();
        this.clientName = clientSocket.getInetAddress().getHostAddress() + 
                        "/" + clientSocket.getPort();
        this.server = kvServer;
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
        KVMessage msg = new KVMessage(StatusType.INFO, "Connection to KV server established: " + serverName);
        KVMessageTool.sendMessage(msg, output);
    }

    public void operation() throws IOException {
        while(isRunning){
            try {
                KVMessage recMsg = KVMessageTool.receiveMessage(input);
                if (recMsg == null){
                    isRunning = false;
                    return;
                }
                KVMessage sendMsg;
                switch (recMsg.getStatus()){
                    case GET:
                        sendMsg = server.get(recMsg.getKey());
                        KVMessageTool.sendMessage(sendMsg, output);
                        //logger.info("SEND \t<" + clientName + ">: '" + sendMsg.getMessage() + "'");
                        break;
                    case PUT:
                        sendMsg = server.put(recMsg.getKey(), recMsg.getValue());
                        KVMessageTool.sendMessage(sendMsg, output);
                        //logger.info("SEND \t<" + clientName + ">: '" + sendMsg.getMessage() + "'");
                        break;
                    case INFO:
                        break;
                    default:
                        sendMsg = handleFail();
                        KVMessageTool.sendMessage(sendMsg, output);
                        //logger.info("SEND \t<" + clientName + ">: " + recMsg.getStatus() + " ' " + sendMsg.getMessage() + "'");
                }

            } catch (IOException ioe) {
                logger.error("Error! Connection lost!", ioe);
                isRunning = false;
            }
        }
    }

    /* Message Handler */
    private KVMessage handleFail() {
        KVMessage message = new KVMessage(StatusType.FAILED, 
                                "Unknown Request Message Type!");
        logger.info("Unknown Request Message Type!");
        return message;
    }
}
