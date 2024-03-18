package app_kvServer.cache;

import shared.KVPair;
import shared.Constants;
import shared.messages.IKVMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public abstract class Cache {

    protected abstract class CacheNode {
        KVPair pair;
        public CacheNode(KVPair pair) {
            this.pair = pair;
        }
    }

    protected int size;
    protected ArrayList<KVPair> evictList;
    protected HashMap<String, CacheNode> map;

    public Cache(int size) {
        this.size = size;
        this.evictList = new ArrayList<>();
        this.map = new HashMap<>();
    }

    abstract public IKVMessage.StatusType insert(KVPair pair);

    abstract public String get(String key);

    public boolean contains(String key){
        return map.containsKey(key);
    }

    abstract public void clearCache();

    public ArrayList<KVPair> getEvictList(){
        return evictList;
    }

    protected IKVMessage.StatusType getType(KVPair pair) {

        // try to delete
        if (pair.getValue().equals(Constants.DELETE)) {
            // in cache
            if (map.containsKey(pair.getKey())) {
                // deleted already
                if ( map.get(pair.getKey()).pair.getValue().equals(Constants.DELETE)) {
                    return IKVMessage.StatusType.DELETE_ERROR;
                }
                // delete success
                return IKVMessage.StatusType.DELETE_SUCCESS;
            }
            // not in cache, leave the problem to storage
            return IKVMessage.StatusType.PENDING;
        }

        if (map.containsKey(pair.getKey())) {
            return IKVMessage.StatusType.PUT_UPDATE;
        }

        return IKVMessage.StatusType.PENDING;
    }
}
