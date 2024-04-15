package shared;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.math.BigInteger;

import java.nio.charset.StandardCharsets;

import shared.KVUtils;
import shared.Constants;
import shared.messages.IKVMessage;

public class KVSubscribe implements IKVMessage{
    private HashMap<String, List<String>> subscription = new HashMap<>();
    private StatusType type;
    private String message;

    public KVSubscribe(){
        this.type = StatusType.SUBSCRIBE_UPDATE;
        this.message = this.getStringStatus();
    }

    public KVSubscribe(String msg){
        this.type = StatusType.SUBSCRIBE_UPDATE;
        this.message = this.getStringStatus() + " " + msg;
        this.genSubscription(msg);
    }

    private void genMessage(){
        this.message = this.type.toString() + " ";
        for (Map.Entry<String, List<String>> entry : subscription.entrySet()) {
            String key = entry.getKey();
            this.message += key;
            for(String socket : entry.getValue()){
                this.message += "," + socket;
            }
            this.message += ";";
        }
        this.message += "\r\n";
    }

    private void genSubscription(String msg){
        String[] tokens = msg.split(";");
        for(String token : tokens){
            String[] entry = token.split(",");
            String key = entry[0];
            List<String> list = new ArrayList<>();
            for(int i = 1; i < entry.length; i++){
                list.add(entry[i]);
            }
            subscription.put(key, list);
        }
    }

    public List<String> getSubscription(String key){
        return this.subscription.get(key);
    }

    public HashMap<String, List<String>> getSubscription(){
        return this.subscription;
    }

    public boolean containsKey(String key){
        return this.subscription.containsKey(key);
    }

    public void putSubscription(String key, List<String> list){
        this.subscription.put(key, list);
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
        return this.message;
    }

    @Override
    public byte[] getByteMessage(){
        this.genMessage();
        return this.message.getBytes(StandardCharsets.UTF_8);
    }

    public void printKVSubsribe(){
        int i = 0;
        System.out.println("\n===========================");
        System.out.println("current SUBSCRIPTION is: \n");
        for (Map.Entry<String, List<String>> entry : subscription.entrySet()) {
            System.out.println("Subscribe key " + i + ": " + entry.getKey());
            int j = 0;
            for(String socket : entry.getValue()){
                System.out.println("Subsribe client " + j + " of " + entry.getKey() + ":  " + socket);
                j++;
            }
            System.out.println("");
            i++;
        }
        System.out.println("end of the SUBSCRIPTION.\n");
        System.out.println("===========================\n");
    }
}