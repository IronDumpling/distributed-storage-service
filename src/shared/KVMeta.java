package shared;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.math.BigInteger;

import java.nio.charset.StandardCharsets;

import shared.KVUtils;
import shared.Constants;
import shared.Constants.ServerRelation;
import shared.messages.IKVMessage;

public class KVMeta implements IKVMessage{

    /* 
     * Key: address:port
     * Value: 
     * Coordinator [key-from, key-to]
     */
    private Map<String, BigInteger[]> metaWrite = new LinkedHashMap<>();

    /* 
     * Key: address:port
     * Value: 
     * Coordinator [key-from, key-to]
     * Replica_1 [key-from, key-to]
     * Replica_2 [key-from, key-to]
     */
    private Map<String, List<BigInteger[]>> metaRead = new LinkedHashMap<>();
    
    private StatusType type;
    private String message;
    private int version;
    private String updateMsg;

    public KVMeta(){
        this.type = StatusType.META_UPDATE;
        this.message = "";
        this.updateMsg = null;
    }

    public KVMeta(String msseage){
        this.type = StatusType.META_UPDATE;
        genMetaWrite(msseage.split(" ")[1]);
        genMetaRead();
    }

    // message here doesn't include status type
    public KVMeta(StatusType type, String msseage){
        this.type = type;
        genMetaWrite(msseage);
        genMetaRead();
    }

    /* Generator */
    private void genMetaWrite(String msg){
        msg = msg.substring(0, msg.length()-1);
        if(msg.split(" ").length > 1){
            this.updateMsg = msg.split(" ")[0];
            msg = msg.split(" ")[1];
        }else{
            this.updateMsg = msg.trim();
            return;
        }
        KVUtils.printInfo("newest update is "+ this.updateMsg);

        for(String pair: msg.trim().split(";")){
            String key;
            BigInteger[] finalValue = new BigInteger[2];
            if (pair.isEmpty()) continue;
            String[] triple = pair.split(",", 3);
            finalValue[0] = new BigInteger(triple[0]);
            finalValue[1] = new BigInteger(triple[1]);
            key = triple[2];
            this.metaWrite.put(key, finalValue);
        }
    }

    private void genMetaRead(){
        List<String> keys = new ArrayList<>(this.metaWrite.keySet());
        List<BigInteger[]> values = new ArrayList<>(this.metaWrite.values());

        int size = this.metaWrite.size();
        
        if(size == 0){
            return;
        } else if(size == 1){
            for (String key : this.metaWrite.keySet()) {
                this.metaRead.put(key, values);
            }
        } else if(size == 2){
            for (int i = 0; i < size; i++) {
                List<BigInteger[]> valueList = new ArrayList<>();
                valueList.add(values.get(i)); // coordinator
                valueList.add(values.get((i + size - 1) % size)); // replica
                this.metaRead.put(keys.get(i), valueList);
            }
        }else{
            for (int i = 0; i < size; i++) {
                List<BigInteger[]> valueList = new ArrayList<>();
                valueList.add(values.get(i)); // coordinator
                valueList.add(values.get((i + size - 1) % size)); // replica_1
                valueList.add(values.get((i + size - 2) % size)); // replica_2
                this.metaRead.put(keys.get(i), valueList);
            }
        }
    }

    public List<Map.Entry<String, BigInteger[]>> findRelatedServers(String serverId) {
        List<Map.Entry<String, BigInteger[]>> entries = new ArrayList<>(metaWrite.entrySet());
        List<Map.Entry<String, BigInteger[]>> result = new ArrayList<>();


        int size = metaWrite.size();

        // no servers in meta write
        if (size == 0) {
            return result;
        }

        List<String> keys = new ArrayList<>(metaWrite.keySet());
        int index = keys.indexOf(serverId);

        int[] relativePositions;
        switch (size) {
            case 1:
                relativePositions = new int[]{0};
                break;
            case 2:
                relativePositions = new int[]{-1, 0};
                break;
            case 3:
                relativePositions = new int[] {-1, 0, 1};
                break;
            case 4:
                relativePositions = new int[] {-1, 0, 1, 2};
                break;
            default:
                relativePositions = new int[] {-2, -1, 0, 1, 2};
                break;
        }

        for (int pos : relativePositions) {
            int relatedIndex = (index + pos + size) % size;
            result.add(entries.get(relatedIndex));
        }

        return result;
    }

    public int ringPosition(int flatPosition, int ringSize) {
        return (flatPosition + ringSize) % ringSize;
    }

    public String getSuccessor(String serverId) {

        List<String> keys = new ArrayList<>(metaWrite.keySet());
        int size = keys.size();
        int index = keys.indexOf(serverId);

        return keys.get(ringPosition(index + 1, size));
    }

    public String getPredecessor(String serverId) {

        List<String> keys = new ArrayList<>(metaWrite.keySet());
        int size = keys.size();
        int index = keys.indexOf(serverId);

        return keys.get(ringPosition(index - 1, size));
    }

    public List<String> getStorageServers(String serverId) {

        List<String> result = new ArrayList<>();

        List<String> keys = new ArrayList<>(metaWrite.keySet());
        int size = keys.size();

        assert (size != 0);
        int index = keys.indexOf(serverId);

        result.add(keys.get(index));

        if (size >= 2) result.add(keys.get(ringPosition(index + 1, size)));
        if (size >= 3) result.add(keys.get(ringPosition(index + 2, size)));

        return result;

    }

    private void genMessage(){        
        this.message = this.type.toString() +  " ";
        BigInteger[] value;
        String key;
        String[] ipAndPort;
        for (Map.Entry<String, BigInteger[]> entry : this.metaWrite.entrySet()) {
            key = entry.getKey();
            value = entry.getValue();
            ipAndPort = key.split(":");
            this.message += value[0] + "," + value[1] + "," + 
                            ipAndPort[0] + ":" + ipAndPort[1] + ";";
        }
        this.message += "\r\n";
    }
    
    /* Setters */
    public void putMetaWrite(String address, int port, BigInteger[] val){
        this.metaWrite.put(address + ":" + port, val);
    }

    public void putMetaWrite(String key, BigInteger[] val){
        this.metaWrite.put(key, val);
    }

    public void removeMetaWrite(String key){
        this.metaWrite.remove(key);
    }

    public void clearMetaWrite(){
        this.metaWrite.clear();
    }

    public void setUpdateMsg(String updateMsg){
        this.updateMsg = updateMsg;
    }

    /* General Getters */
    public String[] getUpdateMsg(){
        return this.updateMsg.split(",");
    }

    @Override
    public String getKey(){
        return null;
    }

    @Override
    public String getValue(){
        return null;
    }

    @Override
    public StatusType getStatus(){
        return this.type;
    }

    @Override
    public String getStringStatus(){
        return this.type.toString();
    }

    @Override
    public String getMessage(){
        this.genMessage();
        if(this.updateMsg!=null){
            String[] tokens = this.message.split(" ", 2);
            this.message = tokens[0] + " " + this.updateMsg + " " + tokens[1];
        }
        return this.message;
    }

    @Override
    public byte[] getByteMessage(){
        this.genMessage();
        if(this.updateMsg!=null){
            String[] tokens = this.message.split(" ", 2);
            this.message = tokens[0] + " " + this.updateMsg + " " + tokens[1];
        }
        return this.message.getBytes(StandardCharsets.UTF_8);
    }

    /* Getters of Meta Write */
    public Map<String, BigInteger[]> getMetaWrite(){
        return this.metaWrite;
    }

    public BigInteger[] getMetaWrite(String address, int port){
        if(address == "localhost") address = "127.0.0.1";
        
        String searchKey = address+ ":" + port;
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            if (entry.getKey().compareTo(searchKey) == 0){
                return entry.getValue();
            }
        }
        return null;
    }

    public BigInteger[] getMetaWrite(String key){
        String searchKey = key;
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            if (entry.getKey().compareTo(searchKey) == 0){
                return entry.getValue();
            }
        }
        return null;
    }

    public String[] mapKeyWrite(String searchIndex){
        String[] server = null;
        
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            if(KVUtils.keyInKeyRange(entry.getValue(), searchIndex)){
                server = new String[] {
                    entry.getKey().split(":")[0], 
                    entry.getKey().split(":")[1]
                };
                break;
            }
        }
        return server;
    }

    public String getIp(String searchIndex){
        BigInteger searchkey = KVUtils.consistHash(searchIndex);
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            if(entry.getValue()[0].compareTo(entry.getValue()[1]) > 0){
                if(entry.getValue()[0].compareTo(searchkey) < 0 ||
                    entry.getValue()[1].compareTo(searchkey) >= 0){
                        return entry.getKey().split(":")[0];
                }
            }
            if(entry.getValue()[0].compareTo(searchkey) < 0 &&
                entry.getValue()[1].compareTo(searchkey) >= 0){
                    return entry.getKey().split(":")[0];
            }
        }
        return null;
    }

    public String getPort(String searchIndex){
        BigInteger searchkey = KVUtils.consistHash(searchIndex);
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            if(entry.getValue()[0].compareTo(entry.getValue()[1]) > 0){
                if(entry.getValue()[0].compareTo(searchkey) < 0 ||
                    entry.getValue()[1].compareTo(searchkey) >= 0){
                        return entry.getKey().split(":")[1];
                }
            }
            if(entry.getValue()[0].compareTo(searchkey) < 0 &&
                entry.getValue()[1].compareTo(searchkey) >= 0){
                    return entry.getKey().split(":")[1];
            }
        }
        return null;
    }

    /* Getters of Meta Read */
    public Map<String, List<BigInteger[]>> getMetaRead(){
        return this.metaRead;
    }

    public List<BigInteger[]> getMetaRead(String address, int port){
        if(address == "localhost") address = "127.0.0.1";

        String searchKey = address+ ":" + port;
        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            if (entry.getKey().compareTo(searchKey) == 0){
                return entry.getValue();
            }
        }
        return null;
    }

    public List<BigInteger[]> getMetaRead(String key){        
        String searchKey = key;
        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            if (entry.getKey().compareTo(searchKey) == 0){
                return entry.getValue();
            }
        }
        return null;
    }

    public BigInteger[] getReplica1(String key){
        String searchKey = key;
        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            if (entry.getKey().compareTo(searchKey) == 0){
                if(entry.getValue().size() >= 2) 
                    return entry.getValue().get(1);
                else {
                    KVUtils.printError("No replica one!");
                    break;
                }
            }
        }
        return null;
    }

    public BigInteger[] getReplica2(String key){
        String searchKey = key;
        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            if (entry.getKey().compareTo(searchKey) == 0){
                if(entry.getValue().size() >= 3) 
                    return entry.getValue().get(2);
                else {
                    KVUtils.printError("No replica two!");
                    break;
                }
            }
        }
        return null;
    }

    public List<String[]> mapKeyRead(String searchIndex){
        List<String[]> serverList = new ArrayList<>();

        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            for(BigInteger[] val : entry.getValue()){
                if(KVUtils.keyInKeyRange(val, searchIndex)){
                    String[] el = new String[] {
                        entry.getKey().split(":")[0],
                        entry.getKey().split(":")[1]
                    };
                    boolean found = false;
                    for (String[] arr : serverList) {
                        if (Arrays.equals(arr, el)) {
                            found = true;
                            break;
                        }
                    }
                    if(!found) serverList.add(el);
                }
            }
        }
        System.out.println("\nhash value of get key is " + KVUtils.consistHash(searchIndex).toString());
        System.out.println("servers that have the key are: ");
        for(String[] srv : serverList){
            System.out.println(srv[0] + ":" + srv[1]);
        }
        System.out.println("");
        return serverList;
    }
    
    /* Getters of Related Servers */
    public List<String> getStorageServers(BigInteger[] keyRange){
        List<String> serverList = new ArrayList<>();
        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            for(BigInteger[] val : entry.getValue()){
                if(val[0].compareTo(keyRange[0]) == 0 
                && val[1].compareTo(keyRange[1]) == 0){
                    serverList.add(entry.getKey());
                }
            }
        }
        System.out.println("servers that have the key are: ");
        for(String srv : serverList){
            System.out.println(srv);
        }
        System.out.println("");
        return serverList;
    }

    public List<String> getReplicators(String key) { 
        List<String> result = new ArrayList<>();
        List<String> keys = new ArrayList<>(metaWrite.keySet());
        int size = metaWrite.size();
        int index = keys.indexOf(key);
        if (size < 2) return result;

        // replicator 1
        result.add(keys.get((index + size + 1) % size));
        // replicator 2
        if (size > 2) result.add(keys.get((index + size + 2) % size));
        
        return result;
    }

    public ServerRelation getServerRelation(String targetServer, String updateServer){
        if(updateServer == null) updateServer = this.getUpdateMsg()[1];
        ServerRelation relation = ServerRelation.OTHERS;

        int updateIndex = -1;
        int targetIndex = -1;
        int currentIndex = 0;

        for (Map.Entry<String, BigInteger[]> entry : this.metaWrite.entrySet()) {
            if (entry.getKey().equals(updateServer)) {
                updateIndex = currentIndex;
            }
            if (entry.getKey().equals(targetServer)) {
                targetIndex = currentIndex;
            }
            currentIndex++;
            KVUtils.printInfo("server " + currentIndex + " is " + entry.getKey());
        }
        
        if(targetIndex == -1){
            KVUtils.printError("Not found target server: " + targetServer + " in the meta data");
            return null;
        }

        if(updateIndex == -1){
            KVUtils.printError("Not found update server: " + updateServer + " in the meta data");
            return null;
        }
        
        int offset = updateIndex - targetIndex;
        if(offset < 0) offset = this.metaWrite.size() + offset;

        if(offset == 0){
            relation = ServerRelation.MYSELF;
        }else if(offset == 1){
            relation = ServerRelation.SUCCESSOR;
        }else if(offset == this.metaWrite.size() - 1){
            relation = ServerRelation.PREDECESSOR;
        }else if(offset == 2){
            relation = ServerRelation.GRAND_SUCCESSOR;
        }else if(offset == this.metaWrite.size() - 2){
            relation = ServerRelation.GRAND_PREDECESSOR;
        }

        KVUtils.printInfo("relation is " + relation.toString());

        return relation;
    }

    /* Helper */
    public void printKVMeta(){
        int i = 0;
        System.out.println("\n===========================");
        System.out.println("current META WRITE is: \n");
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            System.out.println("key" + i + ": " + entry.getKey());
            System.out.println("value" + i + ": From " + entry.getValue()[0]+" to "+ entry.getValue()[1] + "\n");
            i++;
        }
        System.out.println("end of the META WRITE.\n");

        i = 0;
        System.out.println("\ncurrent META READ is: \n");
        for (Map.Entry<String, List<BigInteger[]>> entry : metaRead.entrySet()) {
            System.out.println("key" + i + ": " + entry.getKey());
            System.out.println("values" + i + ":");
            for(BigInteger[] val : entry.getValue()){
                System.out.println("From " + val[0] + " to " + val[1]);
            }
            System.out.println("");
            i++;
        }
        System.out.println("end of the META READ.");
        System.out.println("===========================\n");
    }
}