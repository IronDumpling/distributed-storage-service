package app_kvServer.cache;

import app_kvServer.KVPair;
import shared.messages.IKVMessage;

import java.util.ArrayList;
import java.util.Objects;

public class LRUCache extends Cache {

    private class LRUNode extends CacheNode{
        LRUNode next;
        LRUNode prev;

        public LRUNode(KVPair pair) {
            super(pair);
            next = null;
            prev = null;
        }
    }

    private LRUNode head;
    private LRUNode tail;

    public LRUCache(int size) {
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
            LRUNode node = (LRUNode) map.get(pair.getKey());
            moveToHead(node);
            if (!Objects.equals(node.pair.getValue(), pair.getValue())) {
                node.pair.setDirty(true);
                node.pair.setValue(pair.getValue());
            }
            return type;
        }
        LRUNode node = new LRUNode(pair);

        // cache is full
        if (map.size() == size) {
            removeTail();
        }

        if (head == null) {
            head = node;
            tail = node;
        } else {
            addToHead(node);
        }

        map.put(pair.getKey(), node);

        return type;
    }

    @Override
    public String get(String key) {
        if (!contains(key)) return null;

        LRUNode node = (LRUNode) map.get(key);
        moveToHead(node);
        return node.pair.getValue();
    }

    @Override
    public ArrayList<KVPair> getEvictList() {
        return evictList;
    }

    /***
     * Helper to put a new/existing node to the head
     * @param node the node to be put on head
     */
    private void addToHead(LRUNode node) {
        node.prev = null;
        node.next = head;
        head.prev = node;
        head = node;
    }

    /***
     * When accessing an existing node, put it to head
     * @param node An existing node in the cache
     */
    private void moveToHead(LRUNode node) {
        if (node == head) return;

        // update the prev node and the next node
        if (node == tail) {
            node.prev.next = null;
            tail = node.prev;
        } else {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }

        // update node itself
        addToHead(node);
    }

    /***
     * remove the tail node when cache is full
     */
    private void removeTail() {
        if (tail == null) return;

        LRUNode node = tail;

        // when there is only 1 node
        if (head == tail) {
            head = null;
            tail = null;
        } else {
            // update tail
            tail.prev.next = node;
            tail = tail.prev;
        }

        // remove from hashmap
        map.remove(node.pair.getKey());

        // tail should be evicted into disk if dirty
        if (node.pair.getDirty()) {
            evictList.add(node.pair);
        }
    }

    @Override
    public void clearCache(){
        while (tail != null) {
            removeTail();
        }
    }
}




