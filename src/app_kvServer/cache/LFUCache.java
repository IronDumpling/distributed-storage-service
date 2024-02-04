package app_kvServer.cache;

import app_kvServer.KVPair;
import shared.messages.IKVMessage;

import java.util.Objects;

public class LFUCache extends Cache{

    private class LFUNode extends CacheNode {

        LFUNode prev;
        LFUNode next;
        int reference;


        public LFUNode(KVPair pair) {
            super(pair);
            prev = null;
            next = null;
            reference = 1;
        }
    }

    private LFUNode head;
    private LFUNode tail;

    public LFUCache(int size) {
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
            LFUNode node = (LFUNode) map.get(pair.getKey());
            reference(node);
            if (!Objects.equals(node.pair.getValue(), pair.getValue())) {
                node.pair.setDirty(true);
                node.pair.setValue(pair.getValue());
            }
            return type;
        }

        LFUNode node = new LFUNode(pair);

        if (map.size() == size) {
            removeTail();
        }

        if (head == null) {
            head = node;
        } else {
            node.prev = tail;
            tail.next = node;
        }
        tail = node;

        map.put(pair.getKey(), node);

        // push node to the most front of the same reference count
        reference(node);

        // set back reference to 1
        node.reference = 1;

        return type;

    }

    @Override
    public String get(String key) {
        if (!contains(key)) return null;

        LFUNode node = (LFUNode) map.get(key);
        reference(node);

        return node.pair.getValue();
    }

    @Override
    public void clearCache() {
        while (tail != null) {
            removeTail();
        }
    }

    private void reference(LFUNode node) {
        node.reference += 1;

        while (node != head && node.reference >= node.prev.reference) {

            // 1 <-> 2 <-> 3 <-> 4, swap 2 and 3 (3 is the node)
            // 1 need to: update next
            // 2 need to: update prev to 3 and next to 4
            // 3 need to: update prev to 1 and next to 2
            // 4 need to: update prev to 2

            LFUNode prev = node.prev;

            // update next of 2 to 4
            prev.next = node.next;
            if (tail == node) {
                tail = prev;
            }

            if (prev.prev != null) {
                // update prev of 3 to 1
                node.prev = prev.prev;
                // update next of 1 to 3
                node.prev.next = node;
            } else {
                // node is now head
                head = node;
                node.prev = null;
            }

            // update prev of 4 to 2
            if (node.next != null) {
                node.next.prev = prev;   
            }

            // update next of 3 to 2
            node.next = prev;

            // update prev of 2 to 3
            prev.prev = node;


        }
    }

    private void removeTail() {
        if (tail == null) return;

        LFUNode node = tail;

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
}
