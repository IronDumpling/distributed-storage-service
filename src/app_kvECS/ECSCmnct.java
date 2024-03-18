package app_kvECS;

import java.util.ArrayList;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.math.BigInteger;

import java.io.IOException;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ecs.ECSNode;
import ecs.IECSNode;

import shared.KVUtils;
import shared.Constants;
import shared.KVMeta;

public class ECSCmnct implements Runnable, IECSClient {
    private static Logger logger = Logger.getRootLogger();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private ArrayList<ECSNode> nodes;
    private ServerSocket serverSocket;
    private boolean isRunning;
    private KVMeta meta;
    private int port;

    public ECSCmnct(int port) {
        this.port = port;
        this.meta = new KVMeta();
        this.nodes = new ArrayList<>();
    }
    
    public void run() {
        isRunning = start();
        while(isRunning){
            try {
                Socket client = serverSocket.accept(); 
                KVUtils.printInfo("Receive new connection request!", logger);
                ECSNode node = new ECSNode(client, this);
                new Thread(node).start();
            } catch (IOException ioe) {
                KVUtils.printError("Unable to establish connection!", ioe, logger);
                shutdown();
            }
        }
    }

    @Override
    public boolean start() {
        KVUtils.printInfo("Initialize ecs ...", logger);
        try {
            serverSocket = new ServerSocket(port);
            KVUtils.printSuccess("ECS listening on port: " + serverSocket.getLocalPort(), logger);  
            return true;
        } catch (IOException ioe) {
            KVUtils.printError("Error! Cannot open server socket of ECS:", ioe, logger);
            if(ioe instanceof BindException)
                KVUtils.printError("Port " + port + " is already bound!", ioe, logger);
            return false;
        }
    }

    @Override
    public boolean shutdown() {
        // TODO: exits the remote processes.
        return false;
    }

    /* Hash Ring Modification */
    @Override
    public void joinServer(ECSNode node) {
        lock.writeLock().lock();
        try {
            this.nodes.add(node);
            node.setHashVal(KVUtils.consistHash(node.getNodeName())); 
            Collections.sort(nodes, Comparator.comparing(ECSNode::getHashVal));
            KVUtils.printInfo("Join server: " + node.getNodeName(), logger);
        } catch(Exception e){
            KVUtils.printError("Unexpected things happen when join new server!", e, logger);
        } finally{
            lock.writeLock().unlock();
        }
        printECS();
    }
    
    @Override
    public void removeServer(ECSNode node) {
        lock.writeLock().lock();
        try {
            this.nodes.remove(node);
            KVUtils.printInfo("Remove server: " + node.getNodeName(), logger);
        } catch(Exception e){
            KVUtils.printError("Unexpected things happen when remove server!", e, logger);
        } finally {
            lock.writeLock().unlock();
        }
        printECS();
    }

    public void inputRemoveServer(String name) {
        ECSNode removeNode = getNodeByKey(name);
        if(removeNode == null){
            KVUtils.printError("The input server doesn't exist!", null, logger);
            return;
        }
        removeNode.remove(false);
    }

    public void inputAddServer(String name) {
        ECSNode removeNode = getNodeByKey(name);
        if(removeNode == null){
            KVUtils.printError("The input server doesn't exist!", null, logger);
            return;
        }
        // TODO: add
    }

    public void allMetaUpdate() throws IOException {
        lock.writeLock().lock();
        try{
            meta.clearMetaWrite();
            if(nodes.size() < 1){
                KVUtils.printInfo("No node in nodes!", logger);
            }else{
                BigInteger[] valPairRev = {nodes.get(nodes.size()-1).getHashVal(), 
                                            nodes.get(0).getHashVal()};
                meta.putMetaWrite(nodes.get(0).getNodeName(), valPairRev);
                for(int i = 1; i < nodes.size(); i++){
                    BigInteger[] valPair = {nodes.get(i-1).getHashVal(),
                                            nodes.get(i).getHashVal()};
                    meta.putMetaWrite(nodes.get(i).getNodeName(), valPair);
                }
            }
        } catch (Exception e){
            KVUtils.printError("Unexpected things happen when generate META", e, logger);
        } finally {
            lock.writeLock().unlock();
        }
                
        lock.writeLock().lock();
        try{
            meta.printKVMeta();
            for(ECSNode nd : nodes){
                nd.getKVStore().metaUpdate(meta);
                KVUtils.printInfo("Send META data to " + nd.getNodeName() + " !", logger);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public ArrayList<ECSNode> getNodes() {
        return nodes;
    }

    @Override
    public ECSNode getNodeByKey(String key) {
        ECSNode target = null;
        lock.readLock().lock();
        try{
            for(ECSNode node : nodes){
                if(node.getNodeName().compareTo(key) == 0){
                    target = node;
                    break;
                }
            }
        } finally{
            lock.readLock().unlock();
        }
        return target;
    }

    public KVMeta getMeta() {
        return meta;
    }

    /* Helper */
    public void printECS(){
        KVUtils.printInfo("\n===========================", null);
        KVUtils.printInfo("current ECS data is: \n", null);
        
        lock.readLock().lock();
        for(ECSNode node : nodes){
            KVUtils.printInfo("Server: " + node.getNodeName() + 
                              "; Hash Value: " + node.getHashVal() + 
                              "; Status: " + node.getServerStatus(), null);
        }
        lock.readLock().unlock();
        
        KVUtils.printInfo("\nend of the ECS data.", null);
        KVUtils.printInfo("===========================\n", null);
    }
}
