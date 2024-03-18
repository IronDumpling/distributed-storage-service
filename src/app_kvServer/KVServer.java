package app_kvServer;

import java.util.*;
import java.net.UnknownHostException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.NumberFormat;

import cmnct_server.KVServerCSocket;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache.Cache;
import app_kvServer.cache.FIFOCache;
import app_kvServer.cache.LFUCache;
import app_kvServer.cache.LRUCache;
import app_kvServer.storage.KVServerStorage;

import cmnct_server.KVServerSSocket;

import ecs.IECSNode;

import shared.KVMeta;
import shared.KVPair;
import shared.KVUtils;
import shared.Constants;
import shared.messages.KVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

public class KVServer implements IKVServer {

	private int cacheSize = Constants.CACHE_SIZE;
	private String cacheStrategy = Constants.CACHE_STRATEGY;
	private int port = 0;
	private String address = Constants.LOCAL_HOST;
	public String dataPath = Constants.PATH_DFILE;
	private String logPath = "logs/server.log";
	private Level logLevel = Level.ALL;
	private static Logger logger = Logger.getRootLogger();

	private boolean isRunning;
	private ServerSocket serverSocket;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private Cache cache;
	private CacheStrategy strategy;

	private KVServerStorage storage;
	private List<Socket> connectedSockets = new ArrayList<>();

	private HashMap<Integer, Socket> serverSockets = new HashMap<>();

	private KVMeta kvMeta;

	private ServerStatus status = ServerStatus.STOPPED;

	private KVServerCSocket ECSSocket = null;

	public enum ServerStatus {
		ACTIVATED,
		IN_TRANSFER,
		STOPPED,
		REMOVE
	}

	private String ECSAddress;

	private int ECSPort;

	/* Entry Point */
	public static void main(String[] args) {
		boolean invalidInput = false;
		boolean hasPort = false;
		boolean hasDatapath = false;
		boolean hasECS = false;

		String address = Constants.LOCAL_HOST;
		Level logLevel = Level.ALL;
		int port = 0;
		String dataPath =  Constants.PATH_DFILE;
		String logPath = "logs/server.log";
		int cacheSize = 10;
		String cacheStrategy = Constants.CACHE_STRATEGY;
		String ECSAddress = "";
		int ECSPort = 0;
		
		try {		
			new LogSetup("logs/server.log", logLevel);

			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
					case "-a":
						if (i + 1 < args.length) {
							address = args[i + 1];
							i++;  
						} else {
							KVUtils.printError("Missing value for -a", null, logger);
							invalidInput = true;
						}
						break;
					case "-p":
						if (i + 1 < args.length) {
							port = Integer.parseInt(args[i + 1]);
							i++; 
							hasPort = true;
						} else {
							KVUtils.printError("Missing value for -p", null, logger);
							invalidInput = true;
						}
						break;
					case "-d":
						if (i + 1 < args.length) {
							dataPath = args[i + 1];
							i++;  
							hasDatapath = true;
						} else {
							KVUtils.printError("Missing value for -d", null, logger);
							invalidInput = true;
						}
						break;
					case "-l":
						if (i + 1 < args.length) {
							logPath = args[i + 1];
							i++;  
						} else {
							KVUtils.printError("Missing value for -l", null, logger);
							invalidInput = true;
						}
						break;
					case "-ll":
						if (i + 1 < args.length) {
							logLevel = KVUtils.getLogLevel(args[i + 1].toUpperCase());
							i++;  
						} else {
							KVUtils.printError("Missing value for -ll", null, logger);
							invalidInput = true;
						}
						break;
					case "-c":
						if (i + 1 < args.length) {
							cacheSize = Integer.parseInt(args[i + 1]);
							i++; 
						} else {
							KVUtils.printError("Missing value for -c", null, logger);
							invalidInput = true;
						}
						break;
					case "-s":
						if (i + 1 < args.length) {
							cacheStrategy= args[i + 1];
							i++; 
						} else {
							KVUtils.printError("Missing value for -s", null, logger);
							invalidInput = true;
						}
						break;
					case "-b":
						if (i + 1 < args.length) {
							String[] pair = args[i + 1].split(":");
							ECSAddress = pair[0];
							ECSPort = Integer.parseInt(pair[1]);
							i++;
							hasECS = true; 
						} else {
							KVUtils.printError("Missing value for -b", null, logger);
							invalidInput = true;
						}
						break;
					default:
						KVUtils.printError("Unknown option: " + args[i], null, logger);
						invalidInput = true;
				}
			}

			if(invalidInput) {
				KVUtils.printError("Invalid number of arguments!", null, logger);
				KVUtils.printInfo("Usage: Server -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>" + 
								" -b <ecs Address> -c <cache size> -s <cache strategy>!", logger);
			} else if(!hasPort || !hasDatapath || !hasECS) {
				KVUtils.printError("Missing Port, data path, or ECS address!", null, logger);
				KVUtils.printInfo("Usage: Server -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>" + 
				" -b <ecs Address> -c <cache size> -s <cache strategy>!", logger);
			} else {
				new KVServer(port, cacheSize, cacheStrategy, address, logLevel, dataPath, logPath, ECSAddress, ECSPort).run();
			}
		} catch (IOException ioe) {
			KVUtils.printError("Unable to initialize logger!", ioe, logger);
			ioe.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			KVUtils.printError("Invalid argument! Not a number!", nfe, logger);
			KVUtils.printInfo("Usage: Server -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>" + 
							" -b <ecs Address> -c <cache size> -s <cache strategy>!", logger);
			System.exit(1);
		} catch (Exception e) {
			KVUtils.printError("Unexpected thing happen when initializing!", e, logger);
			KVUtils.printInfo("Usage: Server -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>" + 
							" -b <ecs Address> -c <cache size> -s <cache strategy>!", logger);
			System.exit(1);
		}
	}

	public KVServer(int port, int cacheSize, String strategy, String address, Level logLevel, 
					String dataPath, String logPath, String ECSAddress, int ECSPort) {
		this(port, cacheSize, strategy);
		this.address = address;
		this.logLevel = logLevel;
		this.dataPath = dataPath;
		this.logPath = logPath;
		this.storage = new KVServerStorage(this.dataPath + "/" + serverId());
		this.ECSSocket = new KVServerCSocket(ECSAddress, ECSPort, port);
	}

	/* Life Cycle */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;

		switch (strategy) {
			case Constants.LRU:
				this.strategy = CacheStrategy.LRU;
				this.cache = new LRUCache(cacheSize);
				break;
			case Constants.FIFO:
				this.strategy = CacheStrategy.FIFO;
				this.cache = new FIFOCache(cacheSize);
				break;
			case Constants.LFU:
				this.strategy = CacheStrategy.LFU;
				this.cache = new LFUCache(cacheSize);
				break;
			default:
				this.strategy = CacheStrategy.None;
				this.cache = null;
				break;
		}
	}

	public String serverId() {
		String temp = address;
		if (temp == "localhost") {
			temp = "127.0.0.1";
		}
		return temp + ":" + port;
	}

	@Override
	public void run(){
		isRunning = initializeServer();
		
		if(serverSocket == null) {
			KVUtils.printError("Server socket not inistialized!", null, logger);
			return;
		}

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				close();
				System.out.println("Shutdown successfully!");
			}
		}));

		while(isRunning){
			try {
				Socket client = serverSocket.accept(); 
				KVServerSSocket connection = new KVServerSSocket(client, this);
				connectedSockets.add(client);

				new Thread(connection).start();
				KVUtils.printInfo("Connected to " + client.getInetAddress().getHostName() +  
								" on port " + client.getPort(), logger);
			} catch (IOException ioe) {
				KVUtils.printError("Unable to establish connection!", ioe, logger);
				close();
			}
		}

		KVUtils.printInfo("Server stopped.", logger);
	}

	private boolean initializeServer() {
		KVUtils.printInfo("Initialize server ...", logger);
		try {
			serverSocket = new ServerSocket(port);
			KVUtils.printSuccess("Server listening on port: " + serverSocket.getLocalPort(), logger);
			ECSSocket.connect();
			KVUtils.printInfo(serverId() + ": Trying to connect to ECS", logger);
//			updateKVMeta(ECSSocket.receiveMetaData());
			//this.status = ServerStatus.ACTIVATED;
			setServerStatus(ServerStatus.ACTIVATED);
			return true;
		} catch (IOException ioe) {
			KVUtils.printError("Cannot open server or ECS socket:", ioe, logger);
			if(ioe instanceof BindException)
				KVUtils.printError("Port " + port + " is already bound!", ioe, logger);
			return false;
		} catch (Exception e) {
			KVUtils.printError("Unexpected things happen when initialize server socket!", e, logger);
			return false;
		}

	}

	@Override
	/* Stop the server */
	public void kill(){
		isRunning = false;
		try {
			ECSSocket.disconnect();
			serverSocket.close();
		} catch (IOException e) {
			KVUtils.printError("Unable to close server socket on port: " + port, e, logger);
		}
	}

	@Override
	/* Close all socket connections, then stop the server */
	public void close(){
		for (Socket socket : connectedSockets) {
            try {
                socket.close();
            } catch (IOException e) {
                KVUtils.printError("Unable to close client socket!", e, logger);
            }
        }

		clearCache();

		storage.onServerShutDown();
		kill();
	}

	public void updateKVMeta(KVMeta kvMeta) {
		ServerStatus updateServerStatus = ServerStatus.ACTIVATED;
//		this.kvMeta = kvMeta;
		KVUtils.printSuccess("Update server metaData!", logger);
		KVUtils.printSuccess("current address and port are"+ this.address + this.port, logger);
		
		/*reconnect server which is labeled as REMOVE before */
		if(this.status == ServerStatus.STOPPED){
			KVUtils.printSuccess("ready to reconnect to server!", logger);
			ECSSocket = new KVServerCSocket(ECSAddress, ECSPort, port);
			try{
				ECSSocket.connect();
			}catch(Exception e){
				KVUtils.printError("Unable to reconnect to ECS!", e, logger);
			}
		    updateServerStatus = ServerStatus.STOPPED;
			dataTransfer(kvMeta, updateServerStatus);
		} else {
			/*server status is STOPPED or ACTIVATED */
			boolean needRemove = false;
			if(kvMeta.getMetaWrite(address, port) == null){
				needRemove = true;
				KVUtils.printInfo("We can not find address in meta data\n ready to REMOVE meself", null);
			}else{
				KVUtils.printInfo("find the address in meta data", null);
			}
			//if(kvMeta.getMetaWrite(address, port) == null){
			if(needRemove){
				kvMeta.printKVMeta();
				updateServerStatus = ServerStatus.STOPPED;
				
			}else{
				updateServerStatus = ServerStatus.ACTIVATED;
				kvMeta.printKVMeta();
			}

			dataTransfer(kvMeta, updateServerStatus);
		}
		

	}

	public boolean inRange(String key) {
		if (kvMeta == null) return false;
		String[] addressPort = kvMeta.readKey(key);
		return serverId().equals(addressPort[0] + ":" + addressPort[1]);

	}

	public boolean writeLock() {
		boolean result = lock.writeLock().tryLock();
		if (!result) {
			if (this.status == ServerStatus.IN_TRANSFER) {
				return true;
			}
			lock.writeLock().lock();
		}
		return false;
	}

	public void writeUnlock() {
		lock.writeLock().unlock();
	}

	public void dataTransfer(KVMeta kvMeta, ServerStatus status) {
		if (this.kvMeta == null) {
			this.kvMeta = kvMeta;
			this.status = status;
			return;
		}
		// logic error: handling the datatransfer status
		//this.status = ServerStatus.IN_TRANSFER;
		setServerStatus(ServerStatus.IN_TRANSFER);
		lock.writeLock().lock();
		// put everything into storage first
		this.clearCache();
		String addressPort = null;
		 for (Map.Entry<String, BigInteger[]> entry : kvMeta.getMetaWrite().entrySet()) {

			 String key = entry.getKey();
			 if (Objects.equals(serverId(), key)) {
				 continue;
			 }
			 BigInteger keyFrom = entry.getValue()[0];
			 BigInteger keyTo = entry.getValue()[1];

			 BigInteger[] localPair = this.kvMeta.getMetaWrite(address, port);
			 if (localPair[0].compareTo(keyFrom) == 0 || localPair[1].compareTo(keyTo) == 0) {
				 addressPort = key;
				 break;
			 }
		 }
		 this.kvMeta = kvMeta;
		 if (addressPort == null) {
			//this.status = status;
			setServerStatus(status);
			lock.writeLock().unlock();
			 return;
		 }
		 String[] pair = addressPort.split(":");
		 String target_address = pair[0];
		 int target_port = Integer.parseInt(pair[1]);

		try {
			KVServerCSocket cSocket = new KVServerCSocket(target_address, target_port, port);
			cSocket.connect();
			KVUtils.printInfo(serverId() + ": trying to connect to server: " + target_address + ":" + target_port, logger);
			BigInteger[] keyRange = kvMeta.getMetaWrite(target_address, target_port);
			clearCache();
			storage.transfer(keyRange[0], keyRange[1], cSocket);
			if (!cSocket.receiveDataTransferSuccess()) {
				KVUtils.printInfo(serverId() + ": cant receive data transfer success", logger);
			}

		} catch (Exception e) {
			Constants.logger.error("Unable to connect to socket");
		}

		//this.status = status;
		setServerStatus(ServerStatus.STOPPED);
		lock.writeLock().unlock();
		storage.deleteDuplicate();
		setServerStatus(status);
		if(status == ServerStatus.STOPPED){
			ECSSocket.disconnect();
			ECSSocket = null;
		}
	}

	/* Database Operation */
	public KVMessage get(String key) {
		String value;
		try {
			value = getKV(key);
		} catch (ServerException e) {
			return new KVMessage(e.status, key);
		}
		return new KVMessage(StatusType.GET_SUCCESS, key, value);
	}

	public KVMessage put(String key, String value) {
		StatusType type;
		try {
			type = putKV(key, value);
		} catch (ServerException e) {
			type = e.status;
		}

		return new KVMessage(type, key, value);
	}

	@Override
    public String getKV(String key) throws ServerException{
		String value = null;

		lock.readLock().lock();

		if (inCache(key)) {
			value = cache.get(key);
		} else {
			KVPair pair = storage.get(key);
			if (pair != null) {
				value = pair.getValue();
			}
		}

		if (value == null || value.equals(Constants.DELETE)) {
			lock.readLock().unlock();
			throw new ServerException(StatusType.GET_ERROR, "Cannot find key in storage server!\n");
		}

		lock.readLock().unlock();

		return value;
	}

	@Override
    public StatusType putKV(String key, String value) throws ServerException{
		StatusType type = StatusType.PUT_SUCCESS;

		if (cache != null) {
			type = cache.insert(new KVPair(key, value, true, -1, -1));
			evict();
		} else {
			type = storage.put(new KVPair(key, value, false, -1, -1));
		}

		if (type.equals(StatusType.PENDING)) {
			type = storage.put(new KVPair(key, value, false, -1, -1));
		}

		return type;
	}

	public KVMeta getMeta(){
		return this.kvMeta;
	}

	public ServerStatus getServerStatus(){
		return this.status;
	}

	public void setServerStatus(ServerStatus status){
		this.status = status;
		StatusType type = KVUtils.transferSeverStatusToStatusType(status);
		try{
			ECSSocket.sendServerCurrentState(type, 0);
			KVUtils.printInfo("Current ServerStatus: " + status.toString(), logger);
		}catch(Exception e){
			logger.error("Unable to send data!");
		}
		
	}

	@Override
    public void clearCache(){
		if (cache == null) return;
		cache.clearCache();
		evict();
	}

	@Override
    public void clearStorage(){
		try {
			storage.clearStorage();
		} catch (IOException e) {
			KVUtils.printError("cannot clear storage!\n", e, logger);
		}
	}

	private void evict() {
		ArrayList<KVPair> list = cache.getEvictList();
		while (!list.isEmpty()) {
			KVPair pair = list.remove(0);
			storage.put(pair);
		}
	}

	/* Getter */
	@Override
	public int getPort(){
		return port;
	}

	public int getECSPort(){
		return ECSPort;
	}


	@Override
	public String getHostname(){
		return address;
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return this.strategy;
	}
	
	@Override
	public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
	public boolean inCache(String key){
		if (cache == null) return false;
		return cache.contains(key);
	}

	@Override
	public boolean inStorage(String key){
		return storage.get(key) != null;
	}
}
