package shared.messages;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import shared.KVMeta;
import shared.messages.IKVMessage;
import shared.messages.KVMessageTool;
import shared.KVPair;

public class KVMessage implements IKVMessage {

    private StatusType type;
    private String message;
    private int version;
    private String key;
	private String value;

    public KVMessage(StatusType type) {
        this.type = type;
        this.key = null;
        this.value = null;
        this.message = this.type.toString() + "\r\n";
    }

    public KVMessage(StatusType type, int version) {
        this.type = type;
        this.key = null;
        this.value = null;
        this.message = this.type.toString() + " "+ version + "\r\n";
    }

    public KVMessage(StatusType type, String msg){
        this.type = type;
        this.key = msg;
        this.value = null;
        this.message = this.type.toString() + " " + msg + "\r\n";
    }

    public KVMessage(StatusType type, String key, String val){
        this.type = type;
        this.key = key;
        this.value = val;
        this.message = this.type.toString() + " " + key + " " + val + "\r\n";
    }

    public KVMessage(StatusType type, List<KVPair> transferData){
        this.type = type;

        this.message = type + " ";
        for(KVPair pair:transferData){
            this.message = this.message + pair.str();
        }

        this.message = this.message + "\r\n";
    }

    public String getKey(){
        return key;
    }
	
    public String getValue(){
        return value;
    }

    @Override
    public StatusType getStatus(){
        return type;
    }

    @Override
    public String getStringStatus(){
        return this.type.toString();
    }

    @Override
    public String getMessage(){
        return message;
    }

    @Override
    public byte[] getByteMessage(){
        return message.getBytes(StandardCharsets.UTF_8);
    }
}