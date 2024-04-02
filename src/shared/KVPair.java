package shared;

import java.nio.charset.StandardCharsets;

import shared.messages.IKVMessage;

public class KVPair implements IKVMessage{

    private String key;
    private String value;
    private Boolean dirty;

    private int originLevel;

    private int originLine;

    public KVPair(String key, String value, Boolean dirty) {
        this.key = key;

        this.value = value;
        this.dirty = dirty;
    }

    public KVPair(String key, String value, Boolean dirty, int originLevel, int originLine) {
        this(key, value, dirty);

        this.originLevel = originLevel;
        this.originLine = originLine;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Boolean getDirty() {
        return dirty;
    }

    public void setDirty(Boolean dirty) {
        this.dirty = dirty;
    }

    public int getOriginLevel() {
        return originLevel;
    }

    public void setOriginLevel(int originLevel) {
        this.originLevel = originLevel;
    }

    public int getOriginLine() {
        return originLine;
    }

    public void setOriginLine(int originLine) {
        this.originLine = originLine;
    }

    public String str() {
        return key + " " + value;
    }

    @Override
    public IKVMessage.StatusType getStatus(){
        return IKVMessage.StatusType.DATA_TRANSFER_SINGLE;
    }

    @Override
    public String getStringStatus() {
        return getStatus().toString();
    }

    @Override
    public String getMessage(){
        return getStringStatus() + " " + str() + "\r\n";
    }

    @Override
    public byte[] getByteMessage(){
        return getMessage().getBytes(StandardCharsets.UTF_8);
    }
}
