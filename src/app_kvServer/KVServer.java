package app_kvServer;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.Collectors;

import app_kvServer.storage.StorageManager;
import app_kvServer.storage.StorageManager.StorageType;;
import cmnct_server.KVServerCSocket;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache.Cache;
import app_kvServer.cache.FIFOCache;
import app_kvServer.cache.LFUCache;
import app_kvServer.cache.LRUCache;

import cmnct_server.KVServerSSocket;

import shared.KVMeta;
import shared.KVPair;
import shared.KVUtils;
import shared.Constants;
import shared.Constants.ServerStatus;
import shared.Constants.ServerUpdate;
import shared.Constants.ServerRelation;
import shared.messages.KVMessage;
import shared.messages.KVMessageTool;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

public class KVServer implements IKVServer {

	private static Logger logger = Logger.getRootLogger();

	private final Lock transferLock = new ReentrantLock();
	private final Condition localUpdate = transferLock.newCondition();
	private boolean localUpdateHasDone = false;

	private final ReadWriteLock storageLock = new ReentrantReadWriteLock();
	private final ReadWriteLock metaLock = new ReentrantReadWriteLock();

	private int port = 0;
	private String address = Constants.LOCAL_HOST_IP;
	public String dataPath = Constants.PATH_DFILE;
	private String logPath = "logs/server.log";
	private Level logLevel = Level.ALL;
	
	private Cache cache;
	private CacheStrategy strategy;
	private int cacheSize = Constants.CACHE_SIZE;
	private String cacheStrategy = Constants.CACHE_STRATEGY;

	private List<KVServerCSocket> waitingSockets = new ArrayList<>();

	private boolean isRunning;
	private ServerStatus status = ServerStatus.STOPPED;

	private KVMeta kvMeta;

	private KVServerCSocket ECSSocket = null;
	private ServerSocket serverSocket;

	private int ECSPort;
	private String ECSAddress;

	private StorageManager storage;
	private List<Socket> connectedSockets = new ArrayList<>();
	private HashMap<Integer, Socket> serverSockets = new HashMap<>();

	/* Entry Point */
	public static void main(String[] args) {
		boolean invalidInput = false;
		boolean hasPort = false;
		boolean hasDatapath = false;
		boolean hasECS = false;

		String address = Constants.LOCAL_HOST_IP;
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
							if (address.equals(Constants.LOCAL_HOST)) {
								address = Constants.LOCAL_HOST_IP;
							}
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
	
	/* =============== LIFE CYCLE =============== */
	public KVServer(int port, int cacheSize, String strategy, String address, Level logLevel,
					String dataPath, String logPath, String ECSAddress, int ECSPort) {
		this(port, cacheSize, strategy);
		this.address = address;
		this.logLevel = logLevel;
		this.dataPath = dataPath;
		this.logPath = logPath;
		this.storage = new StorageManager(this.dataPath + "/" + getServerID());
		this.ECSSocket = new KVServerCSocket(ECSAddress, ECSPort, port);
		this.ECSAddress = ECSAddress;
		this.ECSPort = ECSPort;
	}
	
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

	@Override
	public void run(){
		isRunning = initializeServer();
		final String ECSAddress = this.ECSAddress;
    	final int ECSPort = this.ECSPort;
		final int port = this.port;
		
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
			KVUtils.printInfo(getServerID() + ": Trying to connect to ECS", logger);
			//setServerStatus(ServerStatus.ACTIVATED, ServerUpdate.ADD);
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

	/* =============== Client Socket Methods =============== */
	/* 1. Send DATA_TRANSFER Request */
	public void transfer(List<String> replicators, Boolean useCopy) {
		ArrayList<KVServerCSocket> sockets = new ArrayList<>();
		for (String serverId : replicators) {
			String[] split = serverId.split(":");
			String targetAddress = split[0];
			int targetPort = Integer.parseInt(split[1]);
			KVServerCSocket cSocket = new KVServerCSocket(targetAddress, targetPort, port);
			try {
				cSocket.connect();
				KVUtils.printInfo(getServerID() + ": trying to connect to server: " + 
								targetAddress + ":" + targetPort, logger);
				sockets.add(cSocket);
			} catch (Exception e) {
				Constants.logger.error("Unable to connect to socket");
			}
		}
		storage.transfer(sockets, useCopy);
		waitingSockets.addAll(sockets);
	}

	// TODO: deprecate?
	public String findTransferServer(KVMeta kvMeta) {
		for (Map.Entry<String, BigInteger[]> entry : kvMeta.getMetaWrite().entrySet()) {

			String key = entry.getKey();
			if (Objects.equals(getServerID(), key)) continue;

			BigInteger keyFrom = entry.getValue()[0];
			BigInteger keyTo = entry.getValue()[1];

			BigInteger[] localPair = this.kvMeta.getMetaWrite(address, port);
			if (localPair[0].compareTo(keyFrom) == 0 ||
				localPair[1].compareTo(keyTo) == 0) {
				return key;
			}
		}
		return null;
	}

	// TODO: deprecate?
	// return type: address:port of target
	// check if target server is myself
	public String duplicateTransferred(KVMeta kvMeta) {
		// put everything into storage first
		this.clearCache();
		String addressPort = findTransferServer(kvMeta);

		this.kvMeta = kvMeta;
		if (addressPort == null) return null;
		
		String[] pair = addressPort.split(":");
		String target_address = pair[0];
		int target_port = Integer.parseInt(pair[1]);
		BigInteger[] keyRange = kvMeta.getMetaWrite(target_address, target_port);
		storage.duplicateTransferred(keyRange[0], keyRange[1]);
		return addressPort;
	}

	public void copyData(BigInteger keyFrom, BigInteger keyTo) {
		clearCache();
		storage.duplicateTransferred(keyFrom, keyTo);
	}

	/* 2. Send DATA_TRANSFER_SINGLE Request */
	public void dataTransferSingle(StatusType type, KVPair pair){
		if (type == StatusType.PUT_ERROR || type == StatusType.DELETE_ERROR) {
			KVUtils.printError("Can't PUT or DELETE, don't send replica!");
			return;
		}

		List<String[]> list = kvMeta.mapKeyRead(pair.getKey());

		for (String[] serverId : list) {
			String target_address = serverId[0];
			int target_port = Integer.parseInt(serverId[1]);
			if (target_address.equals(this.address) && target_port == this.port)
				continue;
			
			KVServerCSocket cSocket = new KVServerCSocket(target_address, target_port, port);
			try {
				cSocket.connect();
				cSocket.transferSingleData(pair);
				cSocket.receiveDataTransferSuccess();
				cSocket.disconnect();
			} catch(IOException ioe) {
				KVUtils.printError("Cannot transfer single replicate", ioe, logger);
			} catch (Exception e) {
				KVUtils.printError("Cannot transfer single replicate", e, logger);
			}
		}
	}

	/* =============== Server Socket Methods =============== */
	/* 1. Reveive GET Request */
	public KVMessage get(String key) {
		if (!this.inRangeRead(key)) 
    		return new KVMessage(StatusType.SERVER_NOT_RESPONSIBLE);

		storageLock.readLock().lock();
		String value;
		try {
			value = getKV(key);
		} catch (ServerException e) {
			return new KVMessage(e.status, key);
		} finally{
			storageLock.readLock().unlock();
		}
		
		return new KVMessage(StatusType.GET_SUCCESS, key, value);
	}

	@Override
    public String getKV(String key) throws ServerException{
		String value = null;

		if(inCache(key)){
			value = cache.get(key);
		}else{
			KVPair pair = storage.get(key);
			if (pair != null) value = pair.getValue();
		}

		if (value == null || value.equals(Constants.DELETE)) {
			throw new ServerException(StatusType.GET_ERROR, "Cannot find key in storage server!\n");
		}

		return value;
	}

	/* 2. Receive PUT Request */
	public KVMessage put(String key, String value) {
		if (!this.inRangeWrite(key)) 
			return new KVMessage(StatusType.SERVER_NOT_RESPONSIBLE);

		if (this.status == ServerStatus.IN_TRANSFER) 
			return new KVMessage(StatusType.SERVER_WRITE_LOCK);
		
		storageLock.writeLock().lock();
		StatusType type;
		try {
			type = putKV(key, value);
			dataTransferSingle(type, new KVPair(key, value, false));
		} catch (ServerException e) {
			type = e.status;
		} finally{
			storageLock.writeLock().unlock();
		}

        return new KVMessage(type, key, value);
	}

	@Override
    public StatusType putKV(String key, String value) throws ServerException{
		StatusType type = StatusType.PUT_SUCCESS;

		if (cache != null) {
			type = cache.insert(new KVPair(key, value, true, -1, -1));
			evict();
		} else 
			type = storage.put(new KVPair(key, value, false, -1, -1));

		if (type.equals(StatusType.PENDING)) 
			type = storage.put(new KVPair(key, value, false, -1, -1));

		return type;
	}

	/* 3. Receive DATA_TRANSFER_SINGLE Request */
	public KVMessage recTransferSingle(IKVMessage recMsg) {
		if(!(recMsg instanceof KVPair)) 
			return new KVMessage(StatusType.FAILED);

		KVMessage msg = null;
			
		storageLock.writeLock().lock();
		try{
			KVPair pair = (KVPair)recMsg;
			storage.putTransferData(pair);
			msg = new KVMessage(StatusType.DATA_TRANSFER_SUCCESS);
		}finally{
			storageLock.writeLock().unlock();
		}

		return msg;
	}

	/* 4. Receive DATA_TRANSFER Request */
	public KVMessage recDataTransfer(IKVMessage recMsg){
		KVMessage msg = null;
		
		transferLock.lock();
		try{
			if(!localUpdateHasDone) localUpdate.await();
			
			List<KVPair> pairs = KVMessageTool.convertToKVPairList(recMsg);

			storageLock.writeLock().lock();
			try{
				for(KVPair pair : pairs){
					this.storage.putTransferData(pair);
				}
				msg = new KVMessage(StatusType.DATA_TRANSFER_SUCCESS);
			} catch(Exception e){
				KVUtils.printError("Unexpected thing happen when receiving data transfer", e, logger);
				msg = new KVMessage(StatusType.FAILED);
			} finally{
				storageLock.writeLock().unlock();
			}
		} catch(InterruptedException ie){
			KVUtils.printError("Awaiting in data transfer has been interrupted!", ie, logger);
			msg = new KVMessage(StatusType.FAILED);
		} finally{
			transferLock.unlock();
		}

		return msg;
	}

	/* 5. Receive SERVER_ACTIVE Request */
	public KVMessage finishMetaUpdate(){
		setServerStatus(ServerStatus.STOPPED);
		if (storage.isRemoved()) {
			localUpdateHasDone = false;
			ECSSocket.disconnect();
			ECSSocket = null;
			setServerStatus(ServerStatus.REMOVE);
		} else {
			this.storage.deleteDuplicate();
			localUpdateHasDone = false;
			setServerStatus(ServerStatus.ACTIVATED);
		}

		return new KVMessage(StatusType.INFO, "Meta UPDATE finish!");
	}

	/* 6. Receive META_UPDATE Request */
	public KVMessage updateKVMeta(IKVMessage recMsg) {
		KVMessage msg = null;
		
		if(!(recMsg instanceof KVMeta)){
			KVUtils.printError("Unable to transfer IKVMessage to KVMeta!");
			msg = new KVMessage(StatusType.FAILED);
			return msg;
		}
		KVUtils.printSuccess("Update server metaData of "+ this.getServerID(), logger);

		KVMeta newKVMeta = (KVMeta)recMsg;
		newKVMeta.printKVMeta();

		setServerStatus(ServerStatus.IN_TRANSFER);
		localUpdateHasDone = false;
		transferLock.lock();
		try{
			String updateType = newKVMeta.getUpdateMsg()[0];
			String updateServer = newKVMeta.getUpdateMsg()[1];
			boolean updateIsAdd = (updateType.compareTo(ServerUpdate.ADD.toString()) == 0);
			
			ServerRelation relation = null;
			if(updateIsAdd) relation = newKVMeta.getServerRelation(this.getServerID(), null);
			else relation = this.kvMeta.getServerRelation(this.getServerID(), updateServer);
			if(relation == null){
				KVUtils.printError("Unexpected thing happen when finding relation!");
				msg = new KVMessage(StatusType.FAILED);
				return msg;
			}

			switch(relation){
				case MYSELF:
					if(updateIsAdd) addServerMyself(newKVMeta);
					else removeServerMyself(newKVMeta, updateType);
					break;
				case SUCCESSOR:
					if(updateIsAdd) addServerSuccessor(newKVMeta);
					else removeServerSuccessor(newKVMeta);
					break;
				case PREDECESSOR:
					if(updateIsAdd) addServerPredecessor(newKVMeta);
					else removeServerPredecessor(newKVMeta, updateType);
					break;
				case GRAND_SUCCESSOR:
					if(updateIsAdd) addServerGrandSuccessor(newKVMeta);
					else removeGrandSuccessor(newKVMeta);
					break;
				case GRAND_PREDECESSOR:
					basicLocalUpdates(newKVMeta);
					break;
			}

			metaLock.writeLock().lock();
			this.kvMeta = newKVMeta;
			metaLock.writeLock().unlock();
			localUpdate.signalAll();
			localUpdateHasDone = true;
			msg = new KVMessage(StatusType.INFO, "Local update finish!");
		}finally {
			transferLock.unlock();
		}

		for (KVServerCSocket socket : waitingSockets) {
			socket.receiveDataTransferSuccess();
			socket.disconnect();
		}
		waitingSockets.clear();
		return msg;
	}

	private void addServerMyself(KVMeta kvMeta){

		BigInteger[] metaWrite = kvMeta.getMetaWrite(getServerID());

		this.storage.addStorage(StorageType.COORDINATOR, metaWrite[0], metaWrite[1]);

		basicLocalUpdates(kvMeta);
	}

	private void removeServerMyself(KVMeta kvMeta, String updateType){
		if(updateType.compareTo(ServerUpdate.CRASH.toString()) == 0) return;

		int size = this.kvMeta.getMetaWrite().size();
		if(size == 1) return; // last one standing

		clearCache();

		// the previous successor is now the new coordinator
		String successor = this.kvMeta.getSuccessor(getServerID());

		// transfer to all servers holding the data
		transfer(kvMeta.getStorageServers(successor), false);

		storage.destroyStorage(StorageType.COORDINATOR);
		storage.destroyStorage(StorageType.REPLICA1);
		storage.destroyStorage(StorageType.REPLICA2);

	}

	private void addServerPredecessor(KVMeta kvMeta){

		assert (kvMeta.getMetaWrite().size() >= 3);

		String predecessor = kvMeta.getPredecessor(getServerID());
		BigInteger[] predecessorKeyRange = kvMeta.getMetaWrite(predecessor);
		// filter out the data to successor
		copyData(predecessorKeyRange[0], predecessorKeyRange[1]);

		// transfer data to successor
		transfer(Collections.singletonList(predecessor), true);

		basicLocalUpdates(kvMeta);

		storage.copyDuplicateIntoReplica1();

		// transfer data to replicators
		transfer(kvMeta.getReplicators(getServerID()), false);
	}

	private void removeServerPredecessor(KVMeta kvMeta, String updateType){

		// on crash, predecessor will not send coordinator, copy replica 1 into coordinator
		if(updateType.compareTo(ServerUpdate.CRASH.toString()) == 0){
			storage.copyReplica1IntoCoordinator();
		}

		// send coordinator to replicators
		// NOTE: on normal remove, new keyrange is sent by removed server
		// ON CRASH: Since new keyrange is copied into coordinator, send together
		transfer(kvMeta.getReplicators(getServerID()), false);

		basicLocalUpdates(kvMeta);
	}

	private void addServerSuccessor(KVMeta kvMeta){

		int size = kvMeta.getMetaWrite().size();

		if (size == 2) {
			// predecessor == successor
			String predecessor = kvMeta.getPredecessor(getServerID());
			BigInteger[] predecessorKeyRange = kvMeta.getMetaWrite(predecessor);
			// filter out the data to predecessor
			copyData(predecessorKeyRange[0], predecessorKeyRange[1]);

			// transfer data to predecessor
			transfer(Collections.singletonList(predecessor), true);

		}

		basicLocalUpdates(kvMeta);

		if (size == 2) storage.copyDuplicateIntoReplica1();

		// transfer new coordinator data to replicators
		transfer(kvMeta.getReplicators(getServerID()), false);

	}

	private void removeServerSuccessor(KVMeta kvMeta) {

		basicLocalUpdates(kvMeta);

		transfer(kvMeta.getReplicators(getServerID()), false);

	}

	private void addServerGrandSuccessor(KVMeta kvMeta) {

		basicLocalUpdates(kvMeta);

		// transfer replica to replicator 2 : the new server
		transfer(Collections.singletonList(kvMeta.getUpdateMsg()[1]), false);
	}

	private void removeGrandSuccessor(KVMeta kvMeta) {

		basicLocalUpdates(kvMeta);

		String updatedServer = kvMeta.getUpdateMsg()[1];

		transfer(Collections.singletonList(kvMeta.getSuccessor(updatedServer)), false);
	}


	private void basicLocalUpdates(KVMeta kvMeta){
		List<BigInteger[]> metaRead = kvMeta.getMetaRead(address, port);

		int size = metaRead.size();

		storage.updateCoordinator(metaRead.get(0)[0], metaRead.get(0)[1]);
		if (size == 1) {
			storage.destroyStorage(StorageManager.StorageType.REPLICA1);
			storage.destroyStorage(StorageManager.StorageType.REPLICA2);
		}

		if (size == 2) {
			storage.replaceReplica(StorageManager.StorageType.REPLICA1, metaRead.get(1)[0], metaRead.get(1)[1]);
			storage.destroyStorage(StorageManager.StorageType.REPLICA2);
		}

		if (size == 3) {
			storage.replaceReplica(StorageManager.StorageType.REPLICA1, metaRead.get(1)[0], metaRead.get(1)[1]);
			storage.replaceReplica(StorageManager.StorageType.REPLICA2, metaRead.get(2)[0], metaRead.get(2)[1]);
		}
	}

	/* =============== Setters =============== */
	public void setServerStatus(ServerStatus status){
		this.status = status;
		if (status == ServerStatus.REMOVE) return;
		StatusType type = KVUtils.transferSeverStatusToStatusType(status);
		try{
			ECSSocket.sendServerCurrentState(type, 0);
			KVUtils.printInfo("Current ServerStatus of " + this.getServerID() + " is " + status.toString(), logger);
		}catch(Exception e){
			KVUtils.printError("Unable to send data!", e, logger);
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

	/* =============== Getters =============== */
	@Override
	public int getPort(){
		return port;
	}

	public int getECSPort(){
		return ECSPort;
	}

	public String getServerID() {
		String temp = address;
		if (temp == "localhost") {
			temp = "127.0.0.1";
		}
		return temp + ":" + port;
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

	public KVMeta getMeta(){
		metaLock.readLock().lock();
		KVMeta result = this.kvMeta;
		metaLock.readLock().unlock();
		return result;
	}

	public ServerStatus getServerStatus(){
		return this.status;
	}

	/* =============== Conditions & Helpers =============== */
	private boolean inRangeWrite(String key) {
		if (kvMeta == null) return false;
		BigInteger[] keyRange = kvMeta.getMetaWrite(address, port);
		return KVUtils.keyInKeyRange(keyRange, key);
	}

	private boolean inRangeRead(String key) {
		if (kvMeta == null) return false;
		List<BigInteger[]> keyRangeList = kvMeta.getMetaRead(address, port);
		for(BigInteger[] keyRange : keyRangeList){
			if(KVUtils.keyInKeyRange(keyRange, key))
				return true;
		}
		return false;
	}
}
