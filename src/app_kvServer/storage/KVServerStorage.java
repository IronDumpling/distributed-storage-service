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

import java.util.Collections;
import java.util.Objects;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Map;

public class KVServerStorage {

    private MemoryTable mt;

    private String dataPath;

    private StorageMeta meta;

    public KVServerStorage(String dataPath) {
        mt = new MemoryTable();
        this.dataPath = dataPath;

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

    public IKVMessage.StatusType put(KVPair pair) {

        // check if pair exists in storage, if it's not in cache
        IKVMessage.StatusType type;
        KVPair origin = get(pair.getKey());
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
            if (mt.putAndWrite(currDataPath(meta.nextIndex), pair, null)) {
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

    public KVPair get(String key) {
        String value = mt.get(key);
        if (value != null) return new KVPair(key, value, false);
        int index = meta.nextIndex - 1;

        while (index >= 0) {

            String path_str = currDataPath(index);

            TreeMap<String, String> rbt = MemoryTable.loadTreeMapFromFile(path_str);
            value = rbt.get(key);
            if (value != null) return new KVPair(key, value, false);

            index -= 1;
        }

        index = meta.nextCopy - 1;
        while (index >= 0) {

            String path_str = duplicatedDataPath(index);

            TreeMap<String, String> rbt = MemoryTable.loadTreeMapFromFile(path_str);
            value = rbt.get(key);
            if (value != null) return new KVPair(key, value, false);

            index -= 1;
        }

        return null;
    }

    public void onServerShutDown() {
        if (!mt.isEmpty()) {
            try {
                mt.writeToFile(currDataPath(meta.nextIndex), null);
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

    public void transfer(BigInteger keyFrom, BigInteger keyTo, KVServerCSocket socket) {
        // clear memory table
        if (!mt.isEmpty()) {
            try {
                mt.writeToFile(currDataPath(meta.nextIndex), null);
                meta.nextIndex += 1;
            } catch (IOException e) {
                // TODO: Add error catch
            }
        }
        MemoryTable transferCopy = new MemoryTable();
        int index = 0;
        int newIndex = 0;
        int copyIndex = 0;
        ArrayList<KVPair> buffer = new ArrayList<>();
        while (index < meta.nextIndex) {
            for (Map.Entry<String, String> entry : MemoryTable.loadTreeMapFromFile(currDataPath(index)).entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                BigInteger hashValue = KVUtils.consistHash(key);
                if (isNeedTransfer(keyFrom, keyTo, hashValue)) {
                    try {
                        if (transferCopy.putAndWrite(
                                duplicatedDataPath(copyIndex),
                                new KVPair(key, value, false),
                                buffer
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
                                new KVPair(key, value, false), null)) {
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
            transferCopy.writeToFile(duplicatedDataPath(copyIndex), buffer);
            copyIndex += 1;
            socket.transferData(buffer);
        } catch (Exception e) {
            // TODO: Add error catch
        }

        meta.oldCount = meta.nextIndex - 1;
        meta.nextIndex = newIndex;
        meta.nextCopy = copyIndex;
        meta.version_number = meta.version_number + 1;  // TODO is there a case version number jumps?
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
            meta.nextCopy = 0;
            meta.oldCount = 0;
        } catch (IOException e) {
            // TODO: Add error catch
        }
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

    }
}
