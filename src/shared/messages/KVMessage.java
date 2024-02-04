package shared.messages;

import java.nio.charset.StandardCharsets;

import shared.messages.IKVMessage;
import shared.messages.KVMessageTool;

public class KVMessage implements IKVMessage {

    StatusType stsType;
    String strType;
    String key;
    String value;
    String message;

    public KVMessage(StatusType type, String msg){
        this.strType = KVMessageTool.parseStatusType(type);
        this.stsType = type;
        this.key = msg;
        this.value = null;
        this.message = this.strType + " " + msg + "\r\n";
    }

    public KVMessage(StatusType type, String key, String val){
        this.strType = KVMessageTool.parseStatusType(type);
        this.stsType = type;
        this.key = key;
        this.value = val;
        this.message = this.strType + " " + key + " " + val + "\r\n";
    }

    public KVMessage(String type, String msg){
        this.strType = type;
        this.stsType = KVMessageTool.parseStringType(type);
        this.key = msg;
        this.value = null;
        this.message = type + " " + msg + "\r\n";
    }
    
    public KVMessage(String type, String key, String val){
        this.strType = type;
        this.stsType = KVMessageTool.parseStringType(type);
        this.key = key;
        this.value = val;
        this.message = type + " " + key + " " + val + "\r\n";
    }

    @Override
    public String getKey(){
        return key;
    }
	
    @Override
    public String getValue(){
        return value;
    }

    @Override
    public StatusType getStatus(){
        return stsType;
    }

    public String getStringStatus(){
        return strType;
    }

    public String getMessage(){
        return message;
    }

    public byte[] getByteMessage(){
        return message.getBytes(StandardCharsets.UTF_8);
    }
}