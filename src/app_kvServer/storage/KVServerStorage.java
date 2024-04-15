package app_kvServer.storage;

import cmnct_server.KVServerCSocket;

import shared.KVPair;
import shared.Constants;
import shared.KVUtils;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;

public class KVServerStorage {

    private MemoryTable mt;

    private String dataPath;

    private StorageMeta meta;

    private BigInteger keyFrom;
    private BigInteger keyTo;

    private List<Map.Entry<String, String>> duplicatedTable = new ArrayList<>();
    private List<Map.Entry<String, String>> duplicatedTransferredTable = new ArrayList<>();

    public KVServerStorage(String dataPath, BigInteger keyFrom, BigInteger keyTo) {
        mt = new MemoryTable();
        this.dataPath = dataPath;

        this.keyFrom = keyFrom;
        this.keyTo = keyTo;

        Path dir_path = Paths.get(dataPath);
        Path meta_path = Paths.get(metaPath());

        if (!Files.exists(meta_path)) {
            try {
                Files.createDirectory(dir_path);
                // create directory meta
                Files.createFile(meta_path);
                Files.write(meta_path, Collections.singletonList("0 0 0 0"));
                this.meta = new StorageMeta();

            } catch (IOException e) {
                Constants.logger.error("Failed to create empty data store.\n");
            }
        } else {
            // read directory meta
            try {
                this.meta = new StorageMeta(Files.readString(meta_path).trim());

            } catch (IOException e) {
                Constants.logger.error("Failed to access storage.\n");
            }
        }
    }

    public void updateRange(BigInteger keyFrom, BigInteger keyTo) {
        this.keyFrom = keyFrom;
        this.keyTo = keyTo;
    }

    public BigInteger getKeyFrom() {
        return keyFrom;
    }

    public BigInteger getKeyTo() {
        return keyTo;
    }

    public boolean sameRange(BigInteger keyFrom, BigInteger keyTo) {
        return keyFrom.equals(this.keyFrom) && keyTo.equals(this.keyTo);
    }

    public void copyIntoTarget(KVServerStorage target) {

        int index = 0;

        while (index < meta.nextIndex) {
            for (Map.Entry<String, String> entry : MemoryTable.loadTreeMapFromFile(currDataPath(index)).entrySet()) {
                String[] rbtKey = entry.getKey().split("_");
                String table = rbtKey[0];
                String key = rbtKey[1];
                target.put(new KVPair(table, key, entry.getValue(), false));
            }
            index += 1;
        }

        for (Map.Entry<String, String> entry : mt.getAllEntries()) {
            String[] rbtKey = entry.getKey().split("_");
            String table = rbtKey[0];
            String key = rbtKey[1];
            target.put(new KVPair(table, key, entry.getValue(), false));
        }

        for (Map.Entry<String, String> entry: duplicatedTable) {
            target.createTable(entry.getKey(), entry.getValue());
        }
    }

    public void copyDuplicateIntoTarget(KVServerStorage target) {
        int index = 0;

        while (index < meta.nextCopy) {
            for (Map.Entry<String, String> entry : MemoryTable.loadTreeMapFromFile(duplicatedDataPath(index)).entrySet()) {
                String[] rbtKey = entry.getKey().split("_");
                String table = rbtKey[0];
                String key = rbtKey[1];
                target.put(new KVPair(table, key, entry.getValue(), false));
            }
            index += 1;
        }

        for (Map.Entry<String, String> entry: duplicatedTransferredTable) {
            target.createTable(entry.getKey(), entry.getValue());
        }

    }

    public IKVMessage.StatusType put(KVPair pair) {

        System.out.println("putting into storage!");

        // check if pair exists in storage, if it's not in cache
        IKVMessage.StatusType type;
        KVPair origin = get(pair.getRBTKey());
        if (origin != null) {
            if (pair.getValue().equals(Constants.DELETE)) {
                if (origin.getValue().equals(Constants.DELETE)) {
                    return IKVMessage.StatusType.DELETE_ERROR;
                }
                type = IKVMessage.StatusType.DELETE_SUCCESS;
            } else {
                if (origin.getValue().equals(Constants.DELETE)) {
                    type = IKVMessage.StatusType.PUT_SUCCESS;
                } else {
                    type = IKVMessage.StatusType.PUT_UPDATE;
                }
            }
        } else {
            if (pair.getValue().equals(Constants.DELETE)) {
                return IKVMessage.StatusType.DELETE_ERROR;
            }
            type = IKVMessage.StatusType.PUT_SUCCESS;
        }

        try {
            if (mt.putAndWrite(currDataPath(meta.nextIndex), pair)) {
                meta.nextIndex += 1;
            }
        } catch (IOException e) {
            Constants.logger.error("Unable to write to storage\n");

            if (origin == null) return IKVMessage.StatusType.PUT_ERROR;
            if (Objects.equals(pair.getValue(), Constants.DELETE)) return IKVMessage.StatusType.DELETE_ERROR;
            return IKVMessage.StatusType.PUT_ERROR;
        }

        return type;
    }

    public boolean createTable(String tableName, String fields) {

        if (meta.tableEntryMap.containsKey(tableName)) return false;

        meta.tableEntryMap.put(tableName, fields);

        return true;
    }

    public KVPair get(String rbtKey) {
        String value = mt.get(rbtKey);
        String[] rbtKeys = rbtKey.split("_");
        String table = rbtKeys[0];
        String key = rbtKeys[1];
        if (value != null) return new KVPair(table, key, value, false);
        int index = meta.nextIndex - 1;

        while (index >= 0) {

            String path_str = currDataPath(index);

            TreeMap<String, String> rbt = MemoryTable.loadTreeMapFromFile(path_str);
            value = rbt.get(rbtKey);
            if (value != null) return new KVPair(table, key, value, false);

            index -= 1;
        }

        index = meta.nextCopy - 1;
        while (index >= 0) {

            String path_str = duplicatedDataPath(index);

            TreeMap<String, String> rbt = MemoryTable.loadTreeMapFromFile(path_str);
            value = rbt.get(rbtKey);
            if (value != null) return new KVPair(table, key, value, false);

            index -= 1;
        }

        return null;
    }

    public void select(String table, List<String> fields, List<String> list) {

        if (!meta.tableEntryMap.containsKey(table)) return;

        List<String> allFields = new ArrayList<>(Arrays.asList(meta.tableEntryMap.get(table).split(" ")));

        if (fields.size() == 1 && fields.get(0).equals("*")) fields = new ArrayList<>(allFields);

        else if (fields.size() == 2 && fields.get(0).equals("*") && fields.get(1).startsWith("WHERE:")) {
            String condition = fields.get(1);
            fields = new ArrayList<>(allFields);
            fields.add(condition);
        }

        boolean hasCondition = false;

        String conditionField = null;
        String operator = null;
        String conditionValue = null;

        for (String field: fields) {
            if (field.startsWith("WHERE:") && fields.indexOf(field) == fields.size() - 1) {
                hasCondition = true;
                String[] parts = field.split(":", 2);
                String expression = parts[1];
                String[] tokens = expression.split("(?<=[<>=])|(?=[<>=])");
                conditionField = tokens[0];
                operator = tokens[1];
                conditionValue = tokens[2];
                
                if (!allFields.contains(conditionField)) return;
            } else {
                if (!allFields.contains(field)) return;
            }
        }

        ArrayList<String> all = mt.fetchTable(table);

        int index = meta.nextIndex - 1;

        while (index >= 0) {

            String path_str = currDataPath(index);

            TreeMap<String, String> rbt = MemoryTable.loadTreeMapFromFile(path_str);
            all.addAll(MemoryTable.fetchTable(rbt, table));

            index -= 1;
        }

        index = meta.nextCopy - 1;
        while (index >= 0) {

            String path_str = duplicatedDataPath(index);

            TreeMap<String, String> rbt = MemoryTable.loadTreeMapFromFile(path_str);
            all.addAll(MemoryTable.fetchTable(rbt, table));

            index -= 1;
        }

        List<Integer> indices = new ArrayList<>();

        for (String field : fields) {
            if (!field.startsWith("WHERE:"))
                indices.add(allFields.indexOf(field));
        }

        ArrayList<String> allFiltered = new ArrayList<>();
        if (hasCondition) {
            for (String record: all) {
                String[] split = record.split(" ");
                String value = split[allFields.indexOf(conditionField)];
                int compared = value.compareTo(conditionValue);
                switch (operator) {
                    case "<":
                        if (compared < 0)  allFiltered.add(record);
                        break;
                    case "=":
                        if (compared == 0)  allFiltered.add(record);
                        break;
                    case ">":
                        if (compared > 0)  allFiltered.add(record);
                        break;
                }
            }
        } else {
            allFiltered = all;
        }



        for (String record : allFiltered) {
            StringBuilder sb = new StringBuilder();
            String[] split = record.split(" ");
            for (int i : indices) {
                sb.append(split[i]).append(" ");
            }
            list.add(sb.toString().trim());
        }

    }

    public void onServerShutDown() {
        if (!mt.isEmpty()) {
            try {
                mt.writeToFile(currDataPath(meta.nextIndex));
            } catch (IOException e) {
                Constants.logger.error("Unable to write to storage\n");
            }
            meta.nextIndex += 1;
        }
        Path meta_path = Paths.get(metaPath());
        try {
            meta.WriteToMeta(meta_path);
        } catch (IOException e) {
            Constants.logger.error("Unable to write to storage\n");
        }
    }

    public boolean inRange(String key) {
        return KVUtils.keyInKeyRange(new BigInteger[] {keyFrom, keyTo}, key);
    }

    public boolean tableExists(String table) {
        return meta.tableEntryMap.containsKey(table);
    }

    public void copy(BigInteger keyFrom, BigInteger keyTo) {
        // clear memory table
        if (!mt.isEmpty()) {
            try {
                mt.writeToFile(currDataPath(meta.nextIndex));
                meta.nextIndex += 1;
            } catch (IOException e) {
                KVUtils.printError("Cannot copy data!");
            }
        }
        MemoryTable transferCopy = new MemoryTable();
        int index = 0;
        int newIndex = 0;
        int copyIndex = 0;
        while (index < meta.nextIndex) {
            for (Map.Entry<String, String> entry : MemoryTable.loadTreeMapFromFile(currDataPath(index)).entrySet()) {
                String[] rbtKey = entry.getKey().split("_");
                String table = rbtKey[0];
                String key = rbtKey[1];
                String value = entry.getValue();
                BigInteger hashValue = KVUtils.consistHash(key);
                if (isNeedTransfer(keyFrom, keyTo, hashValue)) {
                    try {
                        if (transferCopy.putAndWrite(
                                duplicatedDataPath(copyIndex),
                                new KVPair(table, key, value, false)
                        )) {
                            copyIndex += 1;
                        }
                    } catch (IOException e) {
                        Constants.logger.error("Unable to write rebalence data\n");
                    } catch (Exception e) {
                        Constants.logger.error("Unable to transfer data\n");
                    }
                } else {
                    try {
                        if (mt.putAndWrite(
                                newDataPath(newIndex),
                                new KVPair(table, key, value, false))) {
                            newIndex += 1;
                        }
                    } catch (IOException e) {
                        Constants.logger.error("Unable to write rebalence data\n");
                    }
                }
            }
            index += 1;
        }
        try {
            transferCopy.writeToFile(duplicatedDataPath(copyIndex));
            copyIndex += 1;
        } catch (Exception e) {
            KVUtils.printError("Cannot copy data!");
        }

        for (Map.Entry<String, String> entry: meta.tableEntryMap.entrySet()) {

            if (isNeedTransfer(keyFrom, keyTo, KVUtils.consistHash(entry.getKey()))) {
                duplicatedTransferredTable.add(entry);
            } else {
                duplicatedTable.add(entry);
            }
        }

        meta.oldCount = meta.nextIndex - 1;
        meta.nextIndex = newIndex;
        meta.nextCopy = copyIndex;
        meta.version_number = meta.version_number + 1;
    }

    private static boolean isNeedTransfer(BigInteger keyFrom, BigInteger keyTo, BigInteger hashValue) {
        boolean needTransfer = false;
        if (keyFrom.compareTo(keyTo) == 0) {
            needTransfer = true;
        } else if (keyFrom.compareTo(keyTo) > 0) {
            if (keyFrom.compareTo(hashValue) <= 0 || keyTo.compareTo(hashValue) > 0) {
                needTransfer = true;
            }
        } else {
            if (keyFrom.compareTo(hashValue) <= 0 && keyTo.compareTo(hashValue) > 0) {
                needTransfer = true;
            }
        }
        return needTransfer;
    }

    // replicate for replicators
    public void transfer(List<KVServerCSocket> sockets) {
        // todo can contain a global buffer after data transfer, and reuse
        ArrayList<KVPair> buffer = new ArrayList<>();
        int index = 0;
        while (index < meta.nextIndex) {
            for (Map.Entry<String, String> entry : MemoryTable.loadTreeMapFromFile(currDataPath(index)).entrySet()) {
                String[] rbtKey = entry.getKey().split("_");
                String table = rbtKey[0];
                String key = rbtKey[1];
                String value = entry.getValue();
                buffer.add(new KVPair(table, key, value, false));
                }
            ++index;
        }
        for (Map.Entry<String, String> entry : mt.getAllEntries()) {
            String[] rbtKey = entry.getKey().split("_");
            String table = rbtKey[0];
            String key = rbtKey[1];
            buffer.add(new KVPair(table, key, entry.getValue(), false));
        }
        for (KVServerCSocket socket : sockets) {
            socket.transferData(buffer);
            List<Map.Entry<String, String>> entryList = new ArrayList<>(meta.tableEntryMap.entrySet());
            socket.transferTable(entryList);
        }
    }

    // data transfer
    public void transfer(KVServerCSocket socket) {
        // todo can contain a global buffer after data transfer, and reuse
        ArrayList<KVPair> buffer = new ArrayList<>();
        int index = 0;
        while (index < meta.nextCopy) {
            for (Map.Entry<String, String> entry : MemoryTable.loadTreeMapFromFile(duplicatedDataPath(index)).entrySet()) {
                String[] rbtKey = entry.getKey().split("_");
                String table = rbtKey[0];
                String key = rbtKey[1];
                String value = entry.getValue();
                buffer.add(new KVPair(table, key, value, false));
            }
            ++index;
        }
        socket.transferData(buffer);
        socket.transferTable(duplicatedTransferredTable);
    }

    public void deleteDuplicate() {
        try {
            // delete duplicated data
            int copyIndex = 0;
            while (copyIndex < meta.nextCopy) {
                Path path = Paths.get(duplicatedDataPath(copyIndex));
                Files.delete(path);
                copyIndex -= 1;
            }

            // delete old data
            int oldIndex = 0;
            while (oldIndex < meta.oldCount) {
                Path path = Paths.get(oldDataPath(oldIndex));
                Files.delete(path);
                oldIndex -= 1;
            }
        } catch (IOException e) {
            // TODO: Add error catch
        }
        meta.nextCopy = 0;
        meta.oldCount = 0;

        duplicatedTable.clear();
        duplicatedTransferredTable.clear();
    }

    private String currDataPath(int index) {
        return dataPath + Constants.PATH_DFILE + meta.version_number + index;
    }

    private String newDataPath(int index) {
        return dataPath + Constants.PATH_DFILE + (meta.version_number + 1) + index;
    }

    private String duplicatedDataPath(int index) {
        return dataPath + Constants.PATH_DCopy + index;
    }

    private String oldDataPath(int index) {
        return dataPath + Constants.PATH_DFILE + (meta.version_number - 1)+ index;
    }

    private String metaPath() {
        return dataPath + Constants.PATH_META;
    }

    public void clearStorage() throws IOException {
        int index = meta.nextIndex - 1;
        mt.clearMemoryTable();
        while (index >= 0) {
            Path path = Paths.get(currDataPath(index));
            Files.delete(path);

            index -= 1;
        }
        Path meta = Paths.get(metaPath());
        Files.delete(meta);

        Files.delete(Paths.get(dataPath));
    }
}
