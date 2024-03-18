package app_kvServer.cache;


import shared.KVPair;
import shared.messages.IKVMessage;

import java.util.Objects;

public class FIFOCache extends Cache{

    private class FIFONode extends CacheNode {

        private FIFONode next;

        public FIFONode(KVPair pair) {
            super(pair);
            next = null;
        }
    }

    private FIFONode head;
    private FIFONode tail;

    public FIFOCache(int size) {
        super(size);
        head = null;
        tail = null;
    }

    @Override
    public IKVMessage.StatusType insert(KVPair pair) {

        IKVMessage.StatusType type = getType(pair);

        if (type.equals(IKVMessage.StatusType.DELETE_ERROR)) {
            return type;
        }
        if (contains(pair.getKey())) {
            CacheNode node = map.get(pair.getKey());
            if (!Objects.equals(node.pair.getValue(), pair.getValue())) {
                node.pair.setDirty(true);
                node.pair.setValue(pair.getValue());
            }
            return type;
        }
        FIFONode node = new FIFONode(pair);

        if (map.size() == size) {
            removeHead();
        }

        if (head == null) {
            head = node;
        } else {
            tail.next = node;
        }
        tail = node;

        map.put(pair.getKey(), node);
        return type;
    }

    @Override
    public String get(String key) {

        if (!contains(key)) return null;

        return map.get(key).pair.getValue();
    }

    @Override
    public void clearCache() {
        while (head != null) {
            removeHead();
        }
    }

    private void removeHead() {
        if (head == null) return;

        FIFONode node = head;

        if (head == tail) {
            head = null;
            tail = null;
        } else {
            head = head.next;
        }

        map.remove(node.pair.getKey());

        if (node.pair.getDirty()) {
            evictList.add(node.pair);
        }
    }

}
