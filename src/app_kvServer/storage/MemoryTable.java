package app_kvServer.storage;

import shared.KVPair;
import shared.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/***
 * a temporary memory table that stores MB level data in memory
 * data in the memory table will be marked as in storage
 */
public class MemoryTable {

    private TreeMap<String, String> rbt;

    private int occupied;

    public MemoryTable() {
        rbt = new TreeMap<>();
        occupied = 0;
    }

    /***
     *
     * @param pair kv pair
     * @return true if full, needs to flush into storage, otherwise false
     */
    private boolean put(KVPair pair) {
        rbt.put(pair.getKey(), pair.getValue());
        occupied += pair.getKey().length() + pair.getValue().length();
        return occupied >= Constants.SST_SIZE;
    }

    public String get(String key) {
        // rbt will process a binary search
        return rbt.get(key);
    }

    public void clearMemoryTable() {
        rbt.clear();
    }

    public boolean isEmpty(){
        return occupied == 0;
    }

    public boolean contains(String key) {
        return rbt.containsKey(key);
    }

    public List<Map.Entry<String, String>> getAllEntries() {
        return new ArrayList<>(rbt.entrySet());
    }

    /***
     *
     * @param filename file name
     * @return how many bytes written
     */
    public void writeToFile(String filename, List<KVPair> buffer) throws IOException{
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            for (Map.Entry<String, String> entry : rbt.entrySet()) {
                if (buffer != null) {
                    buffer.add(new KVPair(entry.getKey(), entry.getValue(), false));
                }
                String line = entry.getKey() + " " + entry.getValue();
                writer.write(line);
                writer.newLine();
            }
            rbt.clear();
            occupied = 0;
        }
    }

    public static TreeMap<String, String> loadTreeMapFromFile(String filename) {
        TreeMap<String, String> treeMap = new TreeMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split(" ", 2);
                if (keyValue.length == 2) {
                    treeMap.put(keyValue[0], keyValue[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return treeMap;
    }

    public boolean putAndWrite(String fileName, KVPair pair, List<KVPair> buffer) throws IOException {
        if (put(pair)) {
            writeToFile(fileName, buffer);
            return true;
        }
        return false;
    }
}
