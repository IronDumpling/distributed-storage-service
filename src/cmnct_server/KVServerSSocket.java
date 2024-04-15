package cmnct_server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import java.net.ServerSocket;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;

import shared.KVPair;
import shared.KVMeta;
import shared.KVUtils;
import shared.Constants.ServerStatus;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage.StatusType;

public class KVServerSSocket implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private boolean isRunning;
        
    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    private String serverName;
    private String clientName;

    private KVServer server;

    public KVServerSSocket(Socket clSocket, KVServer kvServer){
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
        IKVMessage msg = new KVMessage(StatusType.INFO, "Connection to KV server established: " + serverName);
        KVMessageTool.sendMessage(msg, output);
        System.out.println("KVServer connected to the client socket "+ serverName +"!");
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

                if (recMsg.getStatus() == StatusType.META) {
                    sendMsg = server.getMeta();
                    while (sendMsg == null) {
                        sendMsg = server.getMeta();
                    }
                    KVMessageTool.sendMessage(sendMsg, output);
                    return;
                }

                if(server.getServerStatus() == ServerStatus.STOPPED){
                    sendMsg = new KVMessage(StatusType.SERVER_STOPPED);
                    KVMessageTool.sendMessage(sendMsg, output);
                    KVUtils.printInfo("Server is STOPPED!");
                    continue;
                }

                if(server.getServerStatus() == ServerStatus.REMOVE) {
                    sendMsg = new KVMessage(StatusType.SERVER_REMOVE);
                    KVMessageTool.sendMessage(sendMsg, output);
                    KVUtils.printInfo("Server is REMOVED!");
                    continue;
                }
                
                switch (recMsg.getStatus()){
                    case GET:
                        sendMsg = server.get(recMsg.getKey());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case PUT:
                        sendMsg = server.put(recMsg.getKey(), recMsg.getValue());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case META:
                        sendMsg = server.getMeta();
                        while (sendMsg == null) {
                            sendMsg = server.getMeta();
                        }
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case META_UPDATE:
                        sendMsg = server.updateKVMeta(recMsg);
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case DATA_TRANSFER:
                        KVUtils.printInfo("receive transferred data");
                        KVUtils.printInfo(recMsg.getMessage());
                        sendMsg = server.recDataTransfer(recMsg);
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case DATA_TRANSFER_SINGLE:
                        KVUtils.printInfo("receive data transfer single");
                        sendMsg = server.recTransferSingle(recMsg);
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case DATA_TRANSFER_TABLE:
                        KVUtils.printInfo("receive transferred table");
                        server.recTableTransfer(recMsg);
                        break;
                    case SERVER_ACTIVE:
                        sendMsg = server.finishMetaUpdate();
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case INFO:
                        KVUtils.printInfo("Receive: " + recMsg.getMessage());
                        break;
                    case CREATE_TABLE:
                        sendMsg = server.createTable(recMsg.getKey(), recMsg.getValue());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case PUT_TABLE:
                        sendMsg = server.putTable(recMsg.getKey(), recMsg.getValue());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case DESTROY_TABLE:
                        sendMsg = server.destroyTable(recMsg.getKey());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case SUBSCRIBE:
                        sendMsg = server.subscribe(recMsg.getKey(), recMsg.getValue());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case UNSUBSCRIBE:
                        sendMsg = server.unsubscribe(recMsg.getKey(), recMsg.getValue());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case SUBSCRIBE_UPDATE:
                        sendMsg = server.updateKVSubscribe(recMsg);
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case SELECT:
                        String[] fields = recMsg.getValue().split(" ");
                        List<String> listOfFields = new ArrayList<>();
                        for(String field: fields){
                            listOfFields.add(field);
                        }
                        System.out.println(listOfFields);
                        sendMsg = server.select(recMsg.getKey(), listOfFields);
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    default:
                        sendMsg = handleFail();
                        System.out.println(recMsg.getMessage());
                        KVMessageTool.sendMessage(sendMsg, output);
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
