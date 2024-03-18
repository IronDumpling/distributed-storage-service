package ecs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.text.NumberFormat;

import org.apache.log4j.Logger;

import ecs.ECSNode;
import app_kvECS.ECSCmnct;
import app_kvServer.KVServer.ServerStatus;

import cmnct_client.KVStore;

import shared.KVUtils;
import shared.Constants;
import shared.KVMeta;
import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessageTool;

public class ECSNode implements IECSNode, Runnable {
    
    private String server;
    private int port = 0;
    private BigInteger hashVal;
    private ServerStatus status = ServerStatus.STOPPED;

    private boolean isRunning = false;
    private Logger logger = Logger.getRootLogger();

    private ECSCmnct ecsCmnct;
    private KVStore kvStore;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input; 

    public ECSNode(Socket clSocket, ECSCmnct ecsCmnct) {
        this.clientSocket = clSocket;
        this.ecsCmnct = ecsCmnct;
        this.isRunning = true;
        this.server = clSocket.getInetAddress().getHostAddress();
    }

    @Override
    public void run(){
        try {
            connect(); 
            operation();
        } catch (IOException ioe) {
            KVUtils.printError("Error! Connection could not be established!", ioe, logger);
            isRunning = false;
        } finally {
            try {
                input.close();
                output.close();
                clientSocket.close();
            } catch (NullPointerException e) {
                KVUtils.printError("Client socket has lost!", e, logger);
            } catch (IOException ioe) {
                KVUtils.printError("Error! Unable to tear down connection!", ioe, logger);
            }
        }
    }

    /* Socket Communication */
    public void connect() throws IOException{
        try{
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            
            IKVMessage msg = new KVMessage(StatusType.INFO, 
                                        "Connection to KV server established: " + getNodeName());
            
            KVMessageTool.sendMessage(msg, output);
            KVUtils.printInfo("Connection established with " + getNodeName(), logger);

            msg = KVMessageTool.receiveMessage(input);
            this.port = Integer.parseInt(msg.getKey());

        } catch (IOException e) {
            KVUtils.printError("Could not establish connection!", e, logger);
            throw e;
        } catch (NumberFormatException nfe) {
            KVUtils.printError("Invalid value for port! Not a number!", nfe, logger);
        }

        try {
            this.kvStore = new KVStore(this.server, this.port);
            this.kvStore.connect();
            ecsCmnct.joinServer(this);
            ecsCmnct.allMetaUpdate();
        } catch(Exception e) {
            KVUtils.printError("Unexpected things happen when joining new server", e, logger);
        }
    }

    /* Communication Actions */
    public void operation() throws IOException {
        while(isRunning){
            try {
                IKVMessage recMsg = KVMessageTool.receiveMessage(input);
                if (recMsg == null){
                    continue;
                }
                
                IKVMessage sendMsg = null;
                switch (recMsg.getStatus()){
                    case SERVER_ACTIVE:
                        sendMsg = updateStatus(ServerStatus.ACTIVATED);
                        break;
                    case SERVER_STOPPED:
                        sendMsg = updateStatus(ServerStatus.STOPPED);
                        break;
                    case SERVER_WRITE_LOCK:
                        sendMsg = updateStatus(ServerStatus.IN_TRANSFER);
                        break;
                    default:
                        sendMsg = handleFail();
                }
                KVMessageTool.sendMessage(sendMsg, output);
            } catch (IOException ioe) {
                if(!kvStore.IsConnected()) {
                    KVUtils.printInfo("Connection to " + getNodeName() + " has lost!", logger);
                } else {
                    KVUtils.printInfo("Unexpected things happen to the connection of " + getNodeName() + "!", logger);
                }
                isRunning = false;
                remove(true);
            }
        }
    }

    public void remove(boolean isPassive){
        KVMessage sendMsg = null;
        this.status = ServerStatus.REMOVE;
        isRunning = false;
        KVUtils.printSuccess(getNodeName() + " update status to " + this.status.toString(), logger);
        try{
            ecsCmnct.removeServer(this);
            ecsCmnct.allMetaUpdate();
            if(!isPassive) kvStore.metaUpdate(ecsCmnct.getMeta());
            KVUtils.printSuccess("Remove server " + getNodeName() + " from the system!", logger);
        } catch (IOException ioe) {
            KVUtils.printError("Failed to remove server " + getNodeName() + "!", ioe, logger);
        }
    }

    public KVMessage updateStatus(ServerStatus status) {
        this.status = status;
        KVMessage info = new KVMessage(StatusType.INFO, "Server status updated to " + status);
        KVUtils.printSuccess(getNodeName() + " update status to " + status.toString(), logger);
        return info;
    }

    private KVMessage handleFail() {
        KVMessage message = new KVMessage(StatusType.FAILED, "Unknown Request Message Type!");
        KVUtils.printInfo("Unknown Request Message Type from " + getNodeName() + "!", logger);
        return message;
    }

    /* Getters */
    @Override
    public String getNodeName() {
        return this.server + ":" + this.port;
    }

    @Override
    public String getNodeHost() {
        return this.server;
    }

    @Override
    public int getNodePort() {
        return this.port;
    }

    @Override
    public BigInteger getHashVal() {
        return this.hashVal;
    }

    public String getServerStatus() {
        return this.status.toString();
    }

    public KVStore getKVStore(){
        return this.kvStore;
    }

    /* Setters */
    public void setHashVal(BigInteger val) {
        this.hashVal = val;
    }

    private void setServerStatus(ServerStatus status) {
        this.status = status;
    }
}
