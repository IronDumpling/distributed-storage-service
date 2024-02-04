package cmnct_client;

import shared.messages.IKVMessage;
import logger.LogSetup;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessageTool;

import constants.Constants;

public class KVStore implements KVCommInterface  {

	private Logger logger = Logger.getRootLogger();
	private boolean running = false;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input; 
	private SocketStatus status = SocketStatus.DISCONNECTED;
	private String server = "";
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	private String address;
	private int port;

	public KVStore(String address, int port) {
		this.address = address;
		this.port =  port;
		this.server = this.address + "/" + this.port;		
	}

	@Override
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
			logger.info("Connection established");
		} catch (UnknownHostException e) {
			logger.error("Unknown Host!", e);
		    setSocketStatus(SocketStatus.DISCONNECTED);
			throw e;
        } catch (IOException e) {
            logger.error("Could not establish connection!", e);
			setSocketStatus(SocketStatus.DISCONNECTED);
			// throw e;
			throw e;
		}
	}

	@Override
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

	@Override
	public IKVMessage put(String key, String value) throws Exception {
		IKVMessage receiveMessage;
		if(!IsConnected()){
			receiveMessage = null;
			throw new IOException("No server connection!");
		}
		
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.PUT, key, value), output);
			logger.info("send PUT request to the sever!");
		} catch (Exception e) {
			logger.error("Unable to send PUT request!");
			disconnect();
			throw e;
		} finally {
			receiveMessage = KVMessageTool.receiveMessage(input);
			logger.info("receive PUT responce from the sever!");
		}
		return receiveMessage;
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		IKVMessage receiveMessage;
		if(!IsConnected()) {
			receiveMessage = null;
			throw new IOException("No server connection!");
		}
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.GET, key), output);
			logger.info("send GET request to the sever!");
		} catch (Exception e) {
			logger.error("Unable to send GET request!");
			disconnect();
			throw e;
		}finally{
			receiveMessage = KVMessageTool.receiveMessage(input);
			logger.info("receive GET respond from the sever!");
		}
		return receiveMessage;
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
			//input.close();
			//output.close();
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
	
	public SocketStatus getSocketStatus(){
		return status;
	}
}
