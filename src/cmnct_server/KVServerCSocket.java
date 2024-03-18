package cmnct_server;

import shared.Constants;
import shared.messages.KVMessageTool;

import logger.LogSetup;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import shared.KVPair;
import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import shared.Constants;
import shared.KVMeta;
import shared.KVUtils;

import java.util.List;

public class KVServerCSocket {

    public enum SocketStatus{
        CONNECTED,
        DISCONNECTED,
        CONNECTION_LOST
    }

    private Logger logger = Logger.getRootLogger();
	private boolean running = false;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input; 
	private SocketStatus status = SocketStatus.DISCONNECTED;
	private String server = "";
	private KVMeta metaData;
	private int serverPort = 0 ;

    /**
	 * Initialize KVServerCSocket with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	private String address;
	private int port;

    public KVServerCSocket(String address, int port, int serverPort) {
		this.address = address;
		this.port =  port;
		this.server = this.address + "/" + this.port;	
		this.metaData = null;	
		this.serverPort = serverPort;
	}

    public void connect() throws Exception {
		if(status == SocketStatus.CONNECTED){
            logger.error("Disconnect first before new connection!", null);
            return;
        }

		if(port < Constants.PORT_LOW || port > Constants.PORT_HIGH){
			logger.error("Invalid port number, should be within " + 
						Constants.PORT_LOW + " and " + Constants.PORT_HIGH, null);
			setSocketStatus(SocketStatus.DISCONNECTED);
			throw new IllegalArgumentException("Invalid port number!");
		}

		try{
			clientSocket = new Socket(address, port);
			setSocketStatus(SocketStatus.CONNECTED);
			setRunning(true);

			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			KVMessageTool.receiveMessage(input);
			KVUtils.printSuccess("Connection established", logger);
			try{
				KVMessageTool.sendMessage(new KVMessage(StatusType.INFO, serverPort), output);
			}catch(Exception e){
				logger.error("Unable to send port!");
				disconnect();
				throw e;
			}
		} catch (UnknownHostException e) {
			logger.error("Unknown Host!", e);
		    setSocketStatus(SocketStatus.DISCONNECTED);
			throw e;
        } catch (IOException e) {
            logger.error("Could not establish connection!", e);
			setSocketStatus(SocketStatus.DISCONNECTED);
			throw e;
		}
	}

    public void disconnect(){

		if(status == SocketStatus.DISCONNECTED){
            logger.error("No server connection!", null);
            return;
        }
        if(status == SocketStatus.CONNECTION_LOST){
            logger.error("Connection to the server has lost!", null);
            return;
        }

		if(!isRunning()){
			logger.error("Server has stopped running", null);
			return;
		} 
		
		logger.info("try to close connection ...");
		try {
			tearDownConnection();
			setSocketStatus(SocketStatus.DISCONNECTED);
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}	
	}

    public IKVMessage transferData(List<KVPair> transferData) throws Exception {
		IKVMessage receiveMessage = null;
		try{ 
			KVMessageTool.sendMessage(new KVMessage(StatusType.DATA_TRANSFER, transferData),output);
		}catch(Exception e){
			logger.error("Unable to send transfer data!");
			disconnect();
			throw e;
		}finally{
			System.out.println("send the transfer data successfully!");
		}	
		return receiveMessage;
	}

    public KVMeta receiveMetaData() throws Exception {
		KVMeta meta = null;
		try{
			IKVMessage receiveMessage = null;
			receiveMessage = KVMessageTool.receiveMessage(input);
			if(receiveMessage instanceof KVMeta){
				meta = (KVMeta) receiveMessage;
				System.out.println("receive the metaData from ECS server!");
			} else{
				System.out.println("Unable to transfer KVMeta from IKVMessage!");
			}
		}catch(Exception e){
			logger.error("Unable to recive metaData from ECS server!");
			disconnect();
			throw e;
		}
		return meta;
	}

    public IKVMessage sendServerCurrentState(StatusType type, int version) throws Exception {
		IKVMessage receiveMessage = null;
		try{
			KVMessageTool.sendMessage(new KVMessage(type,version), output);
			KVUtils.printInfo("Send the status to ECS server!", logger);
		}catch(Exception e){
			logger.error("Unable to send status to ECS server!");
			disconnect();
			throw e;
		}
		return receiveMessage;

	}	public boolean receiveDataTransferSuccess() {
		try {
			KVMessageTool.receiveMessage(input);
			System.out.println("receive DATA_TRANSFER_SUCCESS from the server!");
		} catch (IOException e) {
			return false;
		}
		return true;
	}
	

    public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}

	private boolean IsConnected() throws IOException {
		if(status == SocketStatus.DISCONNECTED){
            logger.error("No server connection!", null);
            return false;
        }
        if(status == SocketStatus.CONNECTION_LOST){
           	logger.error("Connection to the server has lost!", null);
            return false;
        }
		
		boolean isConnected = true;
		try {
			OutputStream output = clientSocket.getOutputStream();
			String myString = "Hello server!";
     		KVMessageTool.sendMessage(new KVMessage(StatusType.INFO, myString), output);
		}catch (IOException ioe) {
			if(isRunning()){
				logger.error("Connection lost!");
				isConnected = false;
				try {
					tearDownConnection();
					setSocketStatus(SocketStatus.CONNECTION_LOST);
				} catch (IOException e) {
					logger.error("Unable to close connection!");
				}
			}
			throw ioe;
		}
		return isConnected;
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

    private void setSocketStatus(SocketStatus st){
        status = st;
        switch (st) {
            case CONNECTED:
                logger.info("Connect to server " + server + "!");
                break;
            case DISCONNECTED:
				logger.info("Disconnect the server " + server + "!");
                break;
            case CONNECTION_LOST:
                logger.error("Connection to the server " + server + " has lost!", null);
                break;
        }
	}



}
