package cmnct_client;

import logger.LogSetup;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessageTool;
import java.net.InetSocketAddress;

import shared.Constants;
import shared.KVMeta;
import shared.KVUtils;
import java.math.BigInteger;

public class KVStore implements KVCommInterface  {

	private Logger logger = Logger.getRootLogger();
	private boolean running = false;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input; 
	private SocketStatus status = SocketStatus.DISCONNECTED;
	private String server = "";
	private KVMeta metaData;
	
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
		this.metaData = null;	
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
		return putInKvMessage(key, value);
	}

	public IKVMessage putInKvMessage(String key, String value) throws Exception{
		IKVMessage receiveMessage = null;
		IKVMessage reply;
		if(!IsConnected()) {
			receiveMessage = null;
			throw new IOException("Unable to connect to server due to NO AVAIABLE SERVER!");	
		}
		
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.PUT, key, value), output);
			logger.info("send PUT request to the sever!");
		} catch (Exception e) {
			logger.error("Unable to send PUT request!");
			disconnect();
			Boolean isConnected = false;
			if(metaData!=null){
				for (Map.Entry<String, BigInteger[]> entry : metaData.getMetaWrite().entrySet()) {
					this.address = entry.getKey().split(":")[0];
					this.port = Integer.parseInt(entry.getKey().split(":")[1]);
					this.server = this.server = this.address + "/" + this.port;
					isConnected = tryConnection();
					if (isConnected == true){
						break;
					}
				}
			}else{
				logger.error("metaData is empty!");
			}
			if(!isConnected){
				disconnect();
				throw e;
			}
			try {
				KVMessageTool.sendMessage(new KVMessage(StatusType.GET, key), output);
				logger.info("send PUT request to the sever!");
			}catch (Exception e2) {
				disconnect();
				throw e2;
			}
		} finally {
			receiveMessage = KVMessageTool.receiveMessage(input);
			if(receiveMessage.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
				try{
					KVMessageTool.sendMessage(new KVMessage(StatusType.META, "Request for META"), output);
				
				}catch (Exception e) {
					logger.error("Unable to send mataData request!");
					disconnect();
					throw e;
				} finally {
					reply = KVMessageTool.receiveMessage(input);
					receiveMessage = reply;
					if(receiveMessage.getStatus() != StatusType.META_UPDATE){
						logger.error("Unable to update metaData!");
						disconnect();
					}

					this.metaData = (KVMeta) reply;
					if(this.metaData!=null){
						logger.info("receive metaData from the sever!");
					}else{
						logger.info("receive empty metaData from the sever!");
					}
	
					String[] addressAndPort = metaData.readKey(key);
					if(addressAndPort == null){
						KVUtils.printInfo("NO record on the server!", null);
					}else{
						disconnect();
						this.address = addressAndPort[0];
						this.port =  Integer.parseInt(addressAndPort[1]);
						this.server = this.address + "/" + this.port;
						connect();
						receiveMessage = putInKvMessage(key,value);
						return receiveMessage;
					}
				}
			}
			logger.info("receive PUT responce from the sever!");
		}
		return receiveMessage;
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		return getInKvMessage(key);
	}

	public IKVMessage getInKvMessage(String key) throws Exception{
		IKVMessage receiveMessage;
		if(!IsConnected()) {
			receiveMessage = null;
			throw new IOException("Unable to connect to server!");
		}
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.GET, key), output);
			logger.info("send GET request to the sever!");
		} catch (Exception e) {
			logger.error("Unable to send GET request!");
			disconnect();
			Boolean isConnected = false;
			if(metaData!=null){
				for (Map.Entry<String, BigInteger[]> entry : metaData.getMetaWrite().entrySet()) {
					this.address = entry.getKey().split(":")[0];
					this.port = Integer.parseInt(entry.getKey().split(":")[1]);
					this.server = this.server = this.address + "/" + this.port;
					isConnected = tryConnection();
					if (isConnected == true){
						break;
					}
				}
			}else{
				logger.error("metaData is empty!");
			}
			if(!isConnected){
				disconnect();
				throw e;
			}
			try {
				KVMessageTool.sendMessage(new KVMessage(StatusType.GET, key), output);
				logger.info("send GET request to the sever!");
			}catch (Exception e2) {
				disconnect();
				throw e2;
			}
		}finally{
			receiveMessage = KVMessageTool.receiveMessage(input);

			if(receiveMessage.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
				try{
					KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
					logger.info("Unable to send mataData request!");
				}catch (Exception e) {
					logger.error("Unable to send mataData request!");
					disconnect();
					throw e;
				} finally {
					IKVMessage reply = KVMessageTool.receiveMessage(input);
					receiveMessage = reply;

					if(receiveMessage.getStatus() != StatusType.META_UPDATE){
						logger.error("Unable to update metaData!");
						disconnect();
					}

					this.metaData = (KVMeta)reply;
					if(this.metaData!=null){
						logger.info("receive metaData from the sever!");
					}else{
						logger.info("receive empty metaData from the sever!");
					}
	
					String[] addressAndPort = metaData.readKey(key);
					if(addressAndPort == null){
						KVUtils.printInfo("NO record on the server!", null);
					}else{
						disconnect();
						this.address = addressAndPort[0];
						this.port =  Integer.parseInt(addressAndPort[1]);
						this.server = this.address + "/" + this.port;
						connect();
						receiveMessage = getInKvMessage(key);
						return receiveMessage;
					}
						
				}

			}
			logger.info("receive GET respond from the sever!");
		}
		return receiveMessage;
	}

	
	public IKVMessage keyrange() throws Exception {
		IKVMessage receiveMessage;
		if(!IsConnected()) {
			receiveMessage = null;
			throw new IOException("Unable to connect to server!");	
		}
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
			logger.info("send metaData request to the sever!");
		} catch (Exception e) {
			logger.error("Unable to send metaData request!");
			disconnect();
			Boolean isConnected = false;
			if(metaData!=null){
				for (Map.Entry<String, BigInteger[]> entry : metaData.getMetaWrite().entrySet()) {
					this.address = entry.getKey().split(":")[0];
					this.port = Integer.parseInt(entry.getKey().split(":")[1]);
					this.server = this.server = this.address + "/" + this.port;
					isConnected = tryConnection();
					if (isConnected == true){
						break;
					}
					logger.error("this address is incorrect!");
				}
			}else{
				logger.error("metaData is empty!");
			}
			if(!isConnected){
				disconnect();
				throw e;
			}

			try {
				KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
				logger.info("send metaData request to the sever!");
			}catch (Exception e2) {
				disconnect();
				throw e2;
			}
			
		}finally{
			receiveMessage = KVMessageTool.receiveMessage(input);
			if(receiveMessage instanceof KVMeta){
				this.metaData = (KVMeta)receiveMessage;
				logger.info("receive new metaData from the sever!");
			}else{
				KVUtils.printError("Receive message is not an instance of KVMeta", null, logger);
			}
		}
		return receiveMessage;
	}

	public boolean tryConnection(){
		try {
			clientSocket = new Socket(address,port);
			setSocketStatus(SocketStatus.CONNECTED);
			setRunning(true);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			KVMessageTool.receiveMessage(input);
			logger.info("Connection established");
			return true;
		} catch (IOException e) {
			this.status = SocketStatus.DISCONNECTED;
			return false;
		}
	}


	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}

	public boolean IsConnected() throws IOException {
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
	
	public SocketStatus getSocketStatus(){
		return status;
	}

	private void redirectConnection(){

	}

	/* For ECS Cmnct */
	public void metaUpdate(KVMeta meta) throws IOException{
		if(!IsConnected()){
			throw new IOException("No server connection!");
		}

		try {
			KVMessageTool.sendMessage(meta, output);
			KVUtils.printSuccess("Send META_UPDATE request!", logger);
		} catch (Exception e) {
			KVUtils.printError("Unable to send META_UPDATE request!", e, logger);
			throw e;
		}
	}
}
