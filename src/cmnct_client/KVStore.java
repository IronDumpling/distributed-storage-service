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
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessageTool;
import java.net.InetSocketAddress;

import shared.Constants;
import shared.KVMeta;
import shared.KVSubscribe;
import shared.KVUtils;
import shared.Constants.ServerStatus;

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
	private IKVMessage receiveMessage;
	private List<String[]> serverList;
	
	private String address;
	private int port;

	public KVStore(String address, int port) {
		this.address = address;
		this.port =  port;
		this.server = this.address + "/" + this.port;	
		this.metaData = null;	
		this.receiveMessage = null;
	}

	@Override
	public void connect() throws Exception {
		if(status == SocketStatus.CONNECTED){
			KVUtils.printError("Disconnect first before new connection!");
            return;
        }

		if(port < Constants.PORT_LOW || port > Constants.PORT_HIGH){
			KVUtils.printError("Invalid port number, should be within " + 
						Constants.PORT_LOW + " and " + Constants.PORT_HIGH);
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
			KVUtils.printInfo("Connection established with "+this.address+":"+this.port);
		} catch (UnknownHostException e) {
			KVUtils.printError("Unknown Host!");
		    setSocketStatus(SocketStatus.DISCONNECTED);
			throw e;
        } catch (IOException e) {
			KVUtils.printError("Could not establish connection!");
			setSocketStatus(SocketStatus.DISCONNECTED);
			throw e;
		}
	}

	@Override
	public void disconnect(){

		if(status == SocketStatus.DISCONNECTED){
			KVUtils.printError("No server connection!");
            return;
        }
        if(status == SocketStatus.CONNECTION_LOST){
			KVUtils.printError("Connection to the server has lost!");
            return;
        }

		if(!isRunning()){
			KVUtils.printError("Server has stopped running");
			return;
		} 

		//KVUtils.printInfo("try to close connection ...");
		try {
			tearDownConnection();
			setSocketStatus(SocketStatus.DISCONNECTED);
		} catch (IOException ioe) {
			KVUtils.printError("Unable to close connection!");
		}	
	}

	/* =============== PUT =============== */
	@Override
	public IKVMessage put(String key, String value) throws Exception {
		receiveMessage = null;
		if(booleanPut(key,value)){
			KVUtils.printInfo("receive PUT responce from the sever successfully!");
		}

		return this.receiveMessage;
	}

	public boolean booleanPut(String key, String value) throws Exception{
		IKVMessage reply = null;
		boolean isFind = false;
		boolean isConnected = false;
		boolean shouldContinue = true;
		String[] pickServer = null;
		int randomIndex = 0;
		Random rand = new Random();

		if(metaData == null){
			//need to update kvMeta
			KVUtils.printError("Meta Data is empty!");

			try{
				KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
				KVUtils.printInfo("send mataData request!");
			}catch (Exception e) {
				KVUtils.printError("Unable to send mataData request!");
				//no kvMeta and connection lost, no way to reconnect to other server 
				disconnect();
			}

			reply = KVMessageTool.receiveMessage(input);
	
			if(reply == null || reply.getStatus() != StatusType.META_UPDATE){
				KVUtils.printError("receive server respond with other STATUSTYPE except META_UPDATE!");
				disconnect();
				return false;
			}
			this.metaData = (KVMeta)reply;
			if(this.metaData!=null){
				KVUtils.printInfo("receive metaData from the server!");
				this.metaData.printKVMeta();
			}else{
				KVUtils.printInfo("receive empty metaData from the server!");
				disconnect();
			}
		}

		//try all the server from the serverList,
		//return true if it can connect
		//return false if no server can connect, need to update 
		
		pickServer = metaData.mapKeyWrite(key);

		if(pickServer == null){
			disconnect();
			KVUtils.printInfo("unable to find corrsponding server!");
		}

		if( this.address.equals(pickServer[0])
			&&this.port == Integer.parseInt(pickServer[1])){
				isConnected = true;
		}else{
			disconnect();
			this.address = pickServer[0];
			this.port = Integer.parseInt(pickServer[1]);
			this.server = this.address + "/" + this.port;
			isConnected = tryConnection();
		}
		
		if(isConnected){
			//isFind = true;
			shouldContinue = sendPut(key,value);
			if(!shouldContinue){
				isConnected = false;
			}
			if(shouldContinue){
				isConnected = receivePut(key,value);
			}

			if(isConnected == true){
				isFind = true;
				return isFind;
			}
		}

		if(!isConnected){
			//1. server shutdown/removed 
			//2. send put request fail 
			//3. get server_NO_RESPOND
			//- should update new kvMeta and then put(key,value)
			for (Map.Entry<String, BigInteger[]> entry : this.metaData.getMetaWrite().entrySet()) {
				String ip = entry.getKey().split(":")[0];
				int port = Integer.parseInt(entry.getKey().split(":")[1]);
				if(this.address.equals(ip) && this.port == port) continue;
				
				this.address = ip;
				this.port = port;
				this.server = this.address + "/" + this.port;
				isConnected = tryConnection();

				if(isConnected) break;
			}

			if(!isConnected){
				KVUtils.printError("Unable to find active server to reconnect!");
				return false;
			}
			this.metaData = null;
			isFind = booleanPut(key, value);
			return isFind;
		}

		return isFind;
	}

	public boolean sendPut(String key, String value) throws Exception{
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.PUT, key, value), output);
			KVUtils.printInfo("Send PUT " + key + " " + value + " request to the server!");
			return true;
		}catch (Exception e){
			KVUtils.printError("Unable to send PUT " + key + " " + value + " request to the server!");
			return false;
		}
	}

	public boolean receivePut(String key, String value) throws Exception{
		IKVMessage reply = null;
		boolean isReceived = false;
		
		try{
			reply = KVMessageTool.receiveMessage(input);
		}catch(Exception e){
			return false;
		}

		if (reply == null) return false;
		KVUtils.printInfo("Receive from " + this.server + ": " + reply.getMessage());

		if(reply.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE 
		&& reply.getStatus() != StatusType.SERVER_STOPPED
		&& reply.getStatus() != StatusType.SERVER_REMOVE){
			this.receiveMessage = reply;
			isReceived = true;
		}

		else if(reply.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
			this.metaData = null;
			isReceived = booleanPut(key,value);
		}
		
		return isReceived;
	}

	/* =============== GET =============== */
	@Override
	public IKVMessage get(String key) throws Exception {
		receiveMessage = null;
		if(booleanGet(key)){
			KVUtils.printInfo("receive GET responce from the sever successfully!");
		}
		return this.receiveMessage;
	}

	public boolean booleanGet(String key) throws Exception{
		IKVMessage reply = null;
		boolean isFind = false;
		boolean isConnected = false;
		boolean shouldContinue = true;
		String[] pickServer = {,};
		int randomIndex = 0;
		Random rand = new Random();

		// need to update kvMeta
		if(metaData == null){
			KVUtils.printError("Meta Data is empty!");

			try{
				KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
				KVUtils.printInfo("Send mataData request to " + this.server + "!");
			}catch (Exception e) {
				KVUtils.printError("Unable to send mataData request!");
				// no kvMeta and connection lost, no way to reconnect to other server 
				disconnect();
			}

			reply = KVMessageTool.receiveMessage(input);
	
			if(reply == null || reply.getStatus() != StatusType.META_UPDATE){
				KVUtils.printError("Receive server respond of other STATUSTYPE except META_UPDATE!");
				disconnect();
			}
			
			this.metaData = (KVMeta)reply;
			if(this.metaData!=null){
				KVUtils.printInfo("receive metaData from the server!");
			}else{
				KVUtils.printInfo("receive empty metaData from the server!");
				disconnect();
			}

			isFind = booleanGet(key);
		}
		
		else{
			// try all the server from the serverList,
			// return true if it can connect
			// return false if no server can connect, need to update 
			this.serverList = metaData.mapKeyRead(key);
			
			// while loop will looping until one server can connect
			while(!isFind){
				// serverList runs out, no server available
				if(this.serverList.size() == 0){
					KVUtils.printInfo("No server available in current kvMeta:");
					KVUtils.printInfo("Need find one avaiable server to update kvMeta");
					for (Map.Entry<String, BigInteger[]> entry : this.metaData.getMetaWrite().entrySet()) {
						String ip = entry.getKey().split(":")[0];
						int port = Integer.parseInt(entry.getKey().split(":")[1]);
						if(this.address.equals(ip) && this.port == port) continue;
						
						this.address = ip;
						this.port = port;
						this.server = this.address + "/" + this.port;
						isConnected = tryConnection();
		
						if(isConnected) break;
					}

					if(!isConnected){
						KVUtils.printError("Unable to find active server to reconnect!");
						return false;
					}

					this.metaData = null;
					isFind = booleanGet(key);
					return isFind;
				}
				
				randomIndex = rand.nextInt(this.serverList.size());
				pickServer= this.serverList.get(randomIndex);
				this.serverList.remove(randomIndex);

				if(this.address == pickServer[0]
				&&this.port == Integer.parseInt(pickServer[1])){
					isConnected = true;
				}else{
					disconnect();
					this.address = pickServer[0];
					this.port = Integer.parseInt(pickServer[1]);
					this.server = this.address + "/" + this.port;
					isConnected = tryConnection();
				}
				
				if(isConnected){
					//isFind = true;
					shouldContinue = sendGet(key);
					if(!shouldContinue) continue;
					isFind = receiveGet(key);
				}
			}
		}
		return isFind;
	}

	public boolean sendGet(String key) throws Exception {
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.GET, key), output);
			KVUtils.printInfo("send GET " + key + " request to the sever!");
			return true;
		} catch (Exception e) {
			// check other server in serverList 
			KVUtils.printError("unable to send GET " + key + " request to the sever!");
			return false;
		}
	}

	public boolean receiveGet(String key) throws Exception {
		IKVMessage reply = null;
		boolean isReceived = false;

		try{
            reply = KVMessageTool.receiveMessage(input);
        }catch(Exception e){
            return false;
        }

		if (reply == null) return false;
		KVUtils.printInfo("Receive from " + this.server + ": " + reply.getMessage());
		
		if(reply.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE 
		&& reply.getStatus() != StatusType.SERVER_STOPPED
		&& reply.getStatus() != StatusType.SERVER_REMOVE){
			this.receiveMessage = reply;
			isReceived = true;
		}

		else if(reply.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
			System.out.println("Not responsible\n\n\n");
			this.metaData.printKVMeta();
			this.metaData = null;
			isReceived = booleanGet(key);
		}
		
		return isReceived;
	}
	
	/* =============== META =============== */
	public IKVMessage keyrange() throws Exception {
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
			KVUtils.printInfo("send metaData request to the sever!");
		} catch (Exception e) {
			KVUtils.printError("Unable to send metaData request!");
			disconnect();
			boolean isConnected = false;

			if(metaData == null){
				KVUtils.printError("metaData is empty!");
			}else{
				for (Map.Entry<String, BigInteger[]> entry : metaData.getMetaWrite().entrySet()) {
					this.address = entry.getKey().split(":")[0];
					this.port = Integer.parseInt(entry.getKey().split(":")[1]);
					this.server = this.address + "/" + this.port;
					isConnected = tryConnection();
					if (isConnected == true) break;
				}
			}

			if(!isConnected){
				KVUtils.printError("No active server!");
				disconnect();
				throw e;
			}

			try {
				KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
				KVUtils.printInfo("send metaData request to the sever!");
			}catch (Exception e2) {
				disconnect();
				throw e2;
			}			
		}finally{
			receiveMessage = KVMessageTool.receiveMessage(input);
			KVUtils.printInfo(receiveMessage.getMessage());
			this.metaData = new KVMeta(receiveMessage.getMessage());
			KVUtils.printInfo("receive new metaData from the sever!");
		}
		return receiveMessage;
	}

	public boolean redirectConnection(String checkRrangeKey){
		IKVMessage reply = null;
		boolean isConnected = false;
		String[] pickServer = null;
		boolean isRedirect = true;
		try{
			KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
			//KVUtils.printInfo("send mataData request!");
		}catch (Exception e) {
			KVUtils.printError("Unable to send mataData request!");
			//no kvMeta and connection lost, no way to reconnect to other server
			if(metaData != null){
				for (Map.Entry<String, BigInteger[]> entry : this.metaData.getMetaWrite().entrySet()) {
					String ip = entry.getKey().split(":")[0];
					int port = Integer.parseInt(entry.getKey().split(":")[1]);
					if(this.address.equals(ip) && this.port == port) continue;
					
					this.address = ip;
					this.port = port;
					this.server = this.address + "/" + this.port;
					isConnected = tryConnection();
	
					if(isConnected) break;
				}
	
				if(!isConnected){
					KVUtils.printError("Unable to find active server to reconnect!");
					return false;
				}

				try{
					KVMessageTool.sendMessage(new KVMessage(StatusType.META), output);
				} catch (Exception e2){
					disconnect();
					return false;
				}
				
			}else{
				KVUtils.printError("Meta Data is empty!");
				disconnect();
				return false;
			}	
		}

		try{
			reply = KVMessageTool.receiveMessage(input);
			while(reply.getMessage() == null){
				reply = KVMessageTool.receiveMessage(input);
			}
			
			//KVUtils.printInfo("Meta Data is received!");
		} catch (Exception e){
			disconnect();
			return false;
		}
		

		if(reply == null || reply.getStatus() != StatusType.META_UPDATE){
			KVUtils.printError("receive server respond with other STATUSTYPE except META_UPDATE!");
			disconnect();
			return false;
		}

		this.metaData = (KVMeta)reply;
		if(this.metaData!=null){
			//KVUtils.printInfo("receive metaData from the server!");
			//this.metaData.printKVMeta();
		}else{
			KVUtils.printInfo("receive empty metaData from the server!");
			disconnect();
		}

		pickServer = metaData.mapKeyWrite(checkRrangeKey);

		if(pickServer == null){
			disconnect();
			KVUtils.printInfo("unable to find corrsponding server!");
		}

		// if( this.address.equals(pickServer[0])
		// 	&&this.port == Integer.parseInt(pickServer[1])){
		// 		isConnected = true;
		// 	KVUtils.printError("don't need to redirected");
		// }else{
		disconnect();
		this.address = pickServer[0];
		this.port = Integer.parseInt(pickServer[1]);
		this.server = this.address + "/" + this.port;
		isConnected = tryConnection();
		// }
		return true;
	}

	/* =============== Subscribe =============== */
	public IKVMessage subscribe(String key, String socketInfo){
		if(redirectConnection(key) == false){
			return null;
		}

		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.SUBSCRIBE, key, socketInfo), output);
			KVUtils.printInfo("send subscription to key: " + key + "!");
		} catch (Exception e) {
			KVUtils.printInfo("Unable to send subscription to key: "+ key+"!");
		}finally{
			try{
				receiveMessage = KVMessageTool.receiveMessage(input);
				KVUtils.printInfo(receiveMessage.getMessage());
			}catch (Exception e) {
				KVUtils.printError("Uanble to receive subscribe result!");
			}
		}			
		return receiveMessage;
	}

	/* =============== Unsubscribe =============== */
	public IKVMessage unsubscribe(String key, String socketInfo){
		if(redirectConnection(key) == false){
			return null;
		}
		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.UNSUBSCRIBE, key, socketInfo), output);
			KVUtils.printInfo("send unsubscription to key: "+ key+" request successfully!");
		} catch (Exception e) {
			KVUtils.printInfo("Unable to send unsubscription to key: "+ key+"!");
		}finally{
			try{
				receiveMessage = KVMessageTool.receiveMessage(input);
				KVUtils.printInfo(receiveMessage.getMessage());
			}catch (Exception e) {
				KVUtils.printError("Uanble to receive unsubscribe result!");
			}
		}
		
		return receiveMessage;
	}
	/* =============== TABLE =============== */
	public IKVMessage create_table(String tableName, String tableFields){
		if(redirectConnection(tableName) == false){
			return null;
		}

		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.CREATE_TABLE, tableName, tableFields), output);
			KVUtils.printInfo("Send create Table:"+tableName+" with Fields: "+ tableFields +" request successfully!");
		} catch (Exception e) {
			KVUtils.printInfo("Unable to send create Table:"+tableName+" with Fields: "+ tableFields +" request!");
		}finally{
			try{
				receiveMessage = KVMessageTool.receiveMessage(input);
				KVUtils.printInfo(receiveMessage.getMessage());
			}catch (Exception e) {
				KVUtils.printError("Uanble to receive create_table result!");
			}
		}
		return receiveMessage;
	}

	public IKVMessage put_table(String tableName, String fields){
		if(redirectConnection(tableName) == false){
			return null;
		}

		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.PUT_TABLE, tableName, fields), output);
			KVUtils.printInfo("Send PUT request " + fields + " in Table: "+tableName+" successfully!");
		} catch (Exception e) {
			KVUtils.printInfo("Uanble to send PUT request " + fields + " in Table: " + tableName + "!");
		}finally{
			try{
				receiveMessage = KVMessageTool.receiveMessage(input);
				KVUtils.printInfo(receiveMessage.getMessage());
			}catch (Exception e) {
				KVUtils.printError("Uanble to receive put_table result!");
			}
		}
		return receiveMessage;
	}

	public IKVMessage destroy_table(String tableName){
		if(redirectConnection(tableName) == false){
			return null;
		}

		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.DESTROY_TABLE, tableName), output);
			KVUtils.printInfo("Send destory Table:" + tableName +" request successfully!");
		} catch (Exception e) {
			KVUtils.printInfo("Unable to Send destory Table:" + tableName +" request!");
		}finally{
			try{
				receiveMessage = KVMessageTool.receiveMessage(input);
				KVUtils.printInfo(receiveMessage.getMessage());
			}catch (Exception e) {
				KVUtils.printError("Uanble to receive destroy_table result!");
			}
		}
		
		return receiveMessage;
	}

	/* =============== SELECT =============== */
	public String[] select(String tableName,String fields){
		if(redirectConnection(tableName) == false){
			return null;
		}

		String[] selectResult = null;
		fields = fields.replace(",", " ");

		try {
			KVMessageTool.sendMessage(new KVMessage(StatusType.SELECT, tableName, fields), output);
			KVUtils.printInfo("Send select Fields: " + fields + " in Table:" + tableName +" request successfully!");
		} catch (Exception e) {
			KVUtils.printInfo("Unable to send select Fields: " + fields + " in Table:" + tableName +" request!");
		}
		try{
			receiveMessage = KVMessageTool.receiveMessage(input);
			//KVUtils.printInfo(receiveMessage.getMessage());
			if(receiveMessage.getStatus() == StatusType.SELECT_SUCCESS){
				
				selectResult = receiveMessage.getMessage().split(" ",2)[1].split("<DELIMITER>");
	
			}else{
				KVUtils.printError("wrong return status!");
				selectResult = null;
			}
		}catch (Exception e) {
			KVUtils.printError("Uanble to receive select result!");
		}
		
		return selectResult;
	}

	/* =============== Helpers =============== */
	public boolean tryConnection(){
		try {
			clientSocket = new Socket(address,port);
			setSocketStatus(SocketStatus.CONNECTED);
			setRunning(true);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			receiveMessage = KVMessageTool.receiveMessage(input);
			if(receiveMessage == null
			|| receiveMessage.getStatus() == StatusType.SERVER_STOPPED 
			|| receiveMessage.getStatus() == StatusType.SERVER_REMOVE ){
				return false;
			}
			//KVUtils.printInfo("Connection established");
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

	private void tearDownConnection() throws IOException {
		setRunning(false);
		//KVUtils.printInfo("tearing down the connection ...");
		if (clientSocket == null) return;
		clientSocket.close();
		clientSocket = null;
		//KVUtils.printInfo("connection closed!");
	}

	private void setSocketStatus(SocketStatus st){
        status = st;
        switch (st) {
            case CONNECTED:
				KVUtils.printInfo("Connect to server " + server + "!");
                break;
            case DISCONNECTED:
				KVUtils.printInfo("Disconnect the server " + server + "!");
                break;
            case CONNECTION_LOST:
				KVUtils.printError("Connection to the server " + server + " has lost!");
                break;
        }
	}
	
	public SocketStatus getSocketStatus(){
		return status;
	}

	/* =============== For ECS Cmnct =============== */
	public void metaUpdate(KVMeta meta) throws IOException{
		try {
			KVMessageTool.sendMessage(meta, output);
			KVUtils.printSuccess("Send META_UPDATE request!");
		} catch (Exception e) {
			KVUtils.printError("Unable to send META_UPDATE request!");
			throw e;
		}
	}

	public void receiveResponse(String type) {
		try {
			IKVMessage rec = KVMessageTool.receiveMessage(input);
			if(rec.getStatus() == StatusType.INFO)
				KVUtils.printSuccess("Receive " + type + " response!");
			else
				KVUtils.printError("Receive incorrect " + type + " response!");
		} catch (IOException e) {
            KVUtils.printError("Unable to receive " + type + " response");
        }
    }

	public void setStatus(ServerStatus status, String msg) throws IOException{
		try {
			StatusType type = KVUtils.transferSeverStatusToStatusType(status);
			KVMessageTool.sendMessage(new KVMessage(type, msg), output);
		} catch (Exception e) {
			KVUtils.printError("Unable to send Server Status Set request!");
			throw e;
		}
	}

	public void subscriptionUpdate(KVSubscribe subscribe) throws IOException {
		try {
			KVMessageTool.sendMessage(subscribe, output);
			KVUtils.printSuccess("Send SUBSCRIBE_UPDATE request!");
		} catch (Exception e) {
			KVUtils.printError("Unable to send SUBSCRIBE_UPDATE request!");
			throw e;
		}
	}
}
