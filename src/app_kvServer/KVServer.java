package app_kvServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;


import app_kvServer.storage.KVStorageHelper;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache.Cache;
import app_kvServer.cache.FIFOCache;
import app_kvServer.cache.LFUCache;
import app_kvServer.cache.LRUCache;

import cmnct_server.KVServerCmnct;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;


import constants.Constants;

public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	*/

	private static Logger logger = Logger.getRootLogger();
	private static int cacheSize = 10;
	private static String cacheStrategy = Constants.FIFO;
	private static int port = 0;
	private static String address = "localhost";
	public static String dataPath = null;
	private static String logPath = "logs/server.log";
	private static Level logLevel = Level.ALL;
	
	private boolean isRunning;
	private ServerSocket serverSocket;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private Cache cache;
	private CacheStrategy strategy;

	private KVStorageHelper storageHelper;
	private List<Socket> connectedSockets = new ArrayList<>();

	/* Entry Point */
	public static void main(String[] args) {
		boolean invalidInput = false;
		boolean hasPort = false;
		boolean hasDatapath = false;
		
		try {		
			new LogSetup("logs/server.log", logLevel);

			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
					case "-p":
						if (i + 1 < args.length) {
							port = Integer.parseInt(args[i + 1]);
							i++; 
							hasPort = true;
						} else {
							printError("Missing value for -p", null);
							invalidInput = true;
						}
						break;
					case "-a":
						if (i + 1 < args.length) {
							address = args[i + 1];
							i++;  
						} else {
							printError("Missing value for -a", null);
							invalidInput = true;
						}
						break;
					case "-d":
						if (i + 1 < args.length) {
							dataPath = args[i + 1];
							i++;  
							hasDatapath = true;
						} else {
							printError("Missing value for -d", null);
							invalidInput = true;
						}
						break;
					case "-l":
						if (i + 1 < args.length) {
							logPath = args[i + 1];
							i++;  
						} else {
							printError("Missing value for -l", null);
							invalidInput = true;
						}
						break;
					case "-ll":
						if (i + 1 < args.length) {
							logLevel = getLogLevel(args[i + 1].toUpperCase());
							i++;  
						} else {
							printError("Missing value for -ll", null);
							invalidInput = true;
						}
						break;
					default:
						printError("Unknown option: " + args[i], null);
						invalidInput = true;
				}
			}

			if(invalidInput || !hasPort || !hasDatapath) {
				printError("Invalid number of arguments!", null);
				printInfo("Usage: Server -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
			} else {
				new KVServer(port, cacheSize, cacheStrategy).run();
			}
		} catch (IOException ioe) {
			printError("Unable to initialize logger!", ioe);
			ioe.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			printError("Invalid argument <port>! Not a number!", nfe);
			printInfo("Usage: Server -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
			System.exit(1);
		}
	}

	/* Life Cycle */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.storageHelper = new KVStorageHelper();
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
		
		if(serverSocket == null) {
			printError("Server socket not inistialized!", null);
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
				KVServerCmnct connection = new KVServerCmnct(client, this);
				connectedSockets.add (client);
				new Thread(connection).start();
				printInfo("Connected to " + 
						client.getInetAddress().getHostName() +  
						" on port " + client.getPort());
			} catch (IOException ioe) {
				printError("Unable to establish connection!", ioe);
				close();
			}
		}

		

		printInfo("Server stopped.");
	}

	private boolean initializeServer() {
		printInfo("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			printSuccess("Server listening on port: " + serverSocket.getLocalPort());    
			return true;
		} catch (IOException e) {
			printError("Error! Cannot open server socket:", e);
			if(e instanceof BindException){
				printError("Port " + port + " is already bound!", e);
			}
			return false;
		}
	}

	@Override
	/* Stop the server */
	public void kill(){
		isRunning = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			printError("Unable to close server socket on port: " + port, e);
		}
	}

	@Override
	/* Close all socket connections, then stop the server */
	public void close(){
		for (Socket socket : connectedSockets) {
            try {
                socket.close();
            } catch (IOException e) {
                printError("Unable to close client socket!", e);
            }
        }
		clearCache();
		kill();
	}

	/* Database Operation */
	public KVMessage get(String key) {
		String value;
		try {
			value = getKV(key);
		} catch (ServerException e) {
			return new KVMessage(e.status, key); // TODO: Use another method to handle error
		}
		return new KVMessage(StatusType.GET_SUCCESS, key, value);
	}

	public KVMessage put(String key, String value) {
		StatusType type;
		try {
			type = putKV(key, value);
		} catch (ServerException e) {
			type = e.status; // TODO: Use another method to handle error
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
			KVPair pair = storageHelper.get(key);
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

		lock.writeLock().lock();

		if (cache != null) {
			type = cache.insert(new KVPair(key, value, true, -1, -1));
			evict();
		} else {
			type = storageHelper.put(new KVPair(key, value, false, -1, -1));
		}

	    // TODO: Combine PUT_PENDING and DELETE_PENDING to PENDING
		if (type.equals(StatusType.PUT_PENDING) || type.equals(StatusType.DELETE_PENDING)) {
			type = storageHelper.put(new KVPair(key, value, false, -1, -1));
		}

		lock.writeLock().unlock();

		return type;
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
			storageHelper.clearStorage();
		} catch (IOException e) {
			printError("cannot clear storage!\n", e);
		}
	}

	private void evict() {
		ArrayList<KVPair> list = cache.getEvictList();
		while (!list.isEmpty()) {
			KVPair pair = list.remove(0);
			storageHelper.put(pair);
		}
	}

	/* Getter */
	@Override
	public int getPort(){
		return port;
	}

	@Override
	public String getHostname(){
		// TODO Auto-generated method stub
		return "127.0.0.1";
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
		return storageHelper.get(key) != null;
	}

	/* Helpers */
	public static void printError(String msg, Exception e){
		System.out.println( "[ERROR]: " + msg);
		if(e != null) logger.error(msg, e);
	}
		
	public static void printSuccess(String msg){
		System.out.println("[SUCCESS]: " + msg);
		logger.info(msg);
	}

	public static void printInfo(String msg){
		System.out.println(msg);
		logger.info(msg);
	}

	public static Level getLogLevel (String logLevel){
		Level level;
		switch (logLevel.toUpperCase()) {
			case "DEBUG":
				level = Level.DEBUG;
				break;
			case "INFO":
				level = Level.INFO;
				break;
			case "WARN":
				level = Level.WARN;
				break;
			case "ERROR":
				level = Level.ERROR;
				break;
			case "FATAL":
				level = Level.FATAL;
				break;
			case "OFF":
				level = Level.OFF;
				break;
			default:
				level = Level.ALL;
				break;
		}
		return level;
	}
}
