package shared;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.math.BigInteger;

import java.nio.charset.StandardCharsets;

import shared.KVUtils;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class KVMeta implements IKVMessage{

    /* 
     * Key: address:port
     * Value: [key-from, key-to]
     */
    private Map<String, BigInteger[]> metaWrite = new HashMap<>();

    /* 
     * Key: address:port
     * Value: ArrayList[[key-from, key-to]]
     */
    private Map<String, List<BigInteger[]>> metaRead = new HashMap<>();
    
    private StatusType type;
    private String message;
    private int version;

    public KVMeta(){
        this.type = StatusType.META_UPDATE;
        this.message = "";
    }

    public KVMeta(String msseage){
        this.type = StatusType.META_UPDATE;
        this.message = msseage;

        message = message.substring(0, message.length()-1);

        for(String pair: msseage.trim().split(";")){
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
        this.setMessage();
        return this.message;
    }

    @Override
    public byte[] getByteMessage(){
        this.setMessage();
        return this.message.getBytes(StandardCharsets.UTF_8);
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

    private void setMessage(){        
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

    /* Getters */
    public Map<String, BigInteger[]> getMetaWrite(){
        return this.metaWrite;
    }

    public BigInteger[] getMetaWrite(String address, int port){
        if(address == "localhost"){
            address = "127.0.0.1";
        }

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

    public String[] readKey(String searchIndex){
        BigInteger keyBigInteger = KVUtils.consistHash(searchIndex);
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            // whole ring
            if (entry.getValue()[0].compareTo(entry.getValue()[1]) == 0) {
                String temp = entry.getKey().split(":")[0];
                if (temp == "localhost") {
                    temp = "127.0.0.1";
                }
                return new String[] {
                        temp,
                        entry.getKey().split(":")[1]
                };
            }
            if(entry.getValue()[0].compareTo(entry.getValue()[1]) > 0){
                if(entry.getValue()[0].compareTo(keyBigInteger) <= 0 ||
                    entry.getValue()[1].compareTo(keyBigInteger) > 0 ){
                        return new String[] {
                            entry.getKey().split(":")[0], 
                            entry.getKey().split(":")[1]
                        };
                }
            }
            if(entry.getValue()[0].compareTo(keyBigInteger) <= 0 &&
                entry.getValue()[1].compareTo(keyBigInteger) > 0 ){
                    return new String[] {
                        entry.getKey().split(":")[0], 
                        entry.getKey().split(":")[1]
                    };
            }
        }
        return null;
    }

    public String getIp(String searchIndex){
        BigInteger searchkey = KVUtils.consistHash(searchIndex);
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            if(entry.getValue()[0].compareTo(entry.getValue()[1]) > 0){
                if(entry.getValue()[0].compareTo(searchkey) < 0 ||
                    entry.getValue()[1].compareTo(searchkey) >= 0 ){
                        return entry.getKey().split(":")[0];
                }
            }
            if(entry.getValue()[0].compareTo(searchkey) < 0 &&
                entry.getValue()[1].compareTo(searchkey) >= 0 ){
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
                    entry.getValue()[1].compareTo(searchkey) >= 0 ){
                        return entry.getKey().split(":")[1];
                }
            }
            if(entry.getValue()[0].compareTo(searchkey) < 0 &&
                entry.getValue()[1].compareTo(searchkey) >= 0 ){
                    return entry.getKey().split(":")[1];
            }
        }
        return null;
    }


    /* Helper */
    public void printKVMeta(){
        int i = 0;
        System.out.println("\n===========================");
        System.out.println("current metaData is: \n");
        for (Map.Entry<String, BigInteger[]> entry : metaWrite.entrySet()) {
            System.out.println("key" + i + ": " + entry.getKey());
            System.out.println("value" + i + ": From " + entry.getValue()[0]+" to "+ entry.getValue()[1] + "\n");
            i++;
        }
        System.out.println("end of the metaData.");
        System.out.println("===========================\n");
    }
}