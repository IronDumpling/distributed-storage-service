package cmnct_server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import java.net.ServerSocket;
import java.util.List;

import shared.KVPair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import app_kvServer.KVServer.ServerStatus;
import shared.KVMeta;
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
                if(server.getServerStatus() == ServerStatus.STOPPED){
                    sendMsg = new KVMessage(StatusType.SERVER_STOPPED);
                    KVMessageTool.sendMessage(sendMsg, output);
                    continue;
                }
                
                switch (recMsg.getStatus()){
                    case GET:
                        if (!server.inRange(recMsg.getKey())) {
                            sendMsg = new KVMessage(StatusType.SERVER_NOT_RESPONSIBLE);
                            KVMessageTool.sendMessage(sendMsg, output);
                            break;
                        }
                        sendMsg = server.get(recMsg.getKey());
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case PUT:
                        if (!server.inRange(recMsg.getKey())) {
                            sendMsg = new KVMessage(StatusType.SERVER_NOT_RESPONSIBLE);
                            KVMessageTool.sendMessage(sendMsg, output);
                            break;
                        }
                        if (server.writeLock()) {
                            sendMsg = new KVMessage(StatusType.SERVER_WRITE_LOCK);
                            KVMessageTool.sendMessage(sendMsg, output);
                            break;
                        }
                        sendMsg = server.put(recMsg.getKey(), recMsg.getValue());
                        server.writeUnlock();
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case META:
                        sendMsg = server.getMeta();
                        KVMessageTool.sendMessage(sendMsg, output);
                        break;
                    case META_UPDATE:
                        System.out.println("receive metaData update!");
                        if(recMsg instanceof KVMeta) server.updateKVMeta((KVMeta)recMsg);
                        else System.out.println("Unable to transfer IIKVMessage to KVMeta!");
                        break;
                    case DATA_TRANSFER:
                        System.out.println("receive transferred data");
                        System.out.println(recMsg.getMessage());
                        server.writeLock();
                        List<KVPair> pairs = KVMessageTool.convertToKVPairList(recMsg);
                        for (KVPair pair : pairs) {
                            server.put(pair.getKey(), pair.getValue());
                        }
                        server.writeUnlock();
                        sendMsg = new KVMessage(StatusType.DATA_TRANSFER_SUCCESS);
                        KVMessageTool.sendMessage(sendMsg, output);

                    case INFO:
                        break;
                    default:
                        sendMsg = handleFail();
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
