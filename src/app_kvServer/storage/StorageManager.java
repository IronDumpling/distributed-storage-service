package app_kvServer.storage;

import cmnct_server.KVServerCSocket;
import shared.Constants;
import shared.KVPair;
import shared.KVUtils;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

public class StorageManager {

    private String dataPath;

    private KVServerStorage coordinator = null;
    private KVServerStorage replica1 = null;
    private KVServerStorage replica2 = null;

    public enum StorageType {
        COORDINATOR,
        REPLICA1,
        REPLICA2;


        @Override
        public String toString() {
            if (this == COORDINATOR) {
                return "coordinator";
            } else if (this == REPLICA1) {
                return "replica1";
            }else {
                return "replica2";
            }
        }
    }

    public StorageManager(String dataPath) {
        this.dataPath = dataPath;
        Path dir_path = Paths.get(dataPath);
        if (Files.exists(dir_path)) {
            try {
                deleteDirectory(dir_path);
            } catch (IOException e) {
                Constants.logger.error("Cannot delete storage directory");
                return;
            }
        }
        try {
            Files.createDirectory(dir_path);
        } catch (IOException e) {
            Constants.logger.error("Cannot create storage directory");
        }
    }

    private void deleteDirectory(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void addStorage(StorageType type, BigInteger keyFrom, BigInteger keyTo) {
        KVServerStorage storage = new KVServerStorage(dataPath + "/" + type.toString(), keyFrom, keyTo);
        switch (type) {
            case COORDINATOR:
                this.coordinator = storage;
                break;
            case REPLICA1:
                this.replica1 = storage;
                break;
            case REPLICA2:
                this.replica2 = storage;
        }
    }

    public void updateCoordinator(BigInteger keyFrom, BigInteger keyTo) {
        assert (coordinator != null);
        coordinator.updateRange(keyFrom, keyTo);
    }

    public void copyReplica1IntoCoordinator() {
        assert (coordinator != null);
        assert (replica1 != null);
        replica1.copyIntoTarget(coordinator);
    }

    public void copyDuplicateIntoReplica1() {
        assert (coordinator != null);
        assert (replica1 != null);
        coordinator.copyDuplicateIntoTarget(replica1);
    }

    public BigInteger[] getKeyRange(StorageType type) {
        assert (coordinator != null);
        // currently this function is only needed for coordinator
        return new BigInteger[]{coordinator.getKeyFrom(), coordinator.getKeyTo()};
    }

    public boolean isRemoved() {
        return coordinator == null;
    }

    public void destroyStorage(StorageType type) {
        try {
            if (type == StorageType.COORDINATOR) {
                assert  (coordinator != null);
                coordinator.clearStorage();
                coordinator = null;
            }
            if (type == StorageType.REPLICA1) {
                if (replica1 != null) {
                    replica1.clearStorage();
                }
                replica1 = null;
            }
            if (type == StorageType.REPLICA2) {
                if (replica2 != null) {
                    replica2.clearStorage();
                }
                replica2 = null;
            }
        } catch (IOException e) {
            Constants.logger.error("Cannot destroy replica");
        }
    }

    public void replaceReplica(StorageType type, BigInteger keyFrom, BigInteger keyTo) {
        if (type == StorageType.REPLICA1) {
            if (replica1 != null && replica1.sameRange(keyFrom, keyTo)) return;
            destroyStorage(type);
            addStorage(type, keyFrom, keyTo);
        }
        if (type == StorageType.REPLICA2) {
            if (replica2 != null && replica2.sameRange(keyFrom, keyTo)) return;
            destroyStorage(type);
            addStorage(type, keyFrom, keyTo);
        }
    }

    public void putTransferData(KVPair pair){
        if(coordinator != null && coordinator.inRange(pair.getKey())){
            KVUtils.printInfo("PUT " + pair.getKey() + " " + pair.getValue() + " to coordinator");
            coordinator.put(pair);
        }
        else if (replica1 != null && replica1.inRange(pair.getKey())){
            KVUtils.printInfo("PUT " + pair.getKey() + " " + pair.getValue() + " to replica1");
            replica1.put(pair);
        }
        else if (replica2 != null && replica2.inRange(pair.getKey())){
            KVUtils.printInfo("PUT " + pair.getKey() + " " + pair.getValue() + " to replica2");
            replica2.put(pair);
        }
        else KVUtils.printError("Can not put transfer data \"" + pair.getKey() + " " + 
                                pair.getValue() + "\" to any storage section!");
    }

    public IKVMessage.StatusType put(KVPair pair) {
        assert (coordinator != null);
        return coordinator.put(pair);
    }

    public KVPair get(String key) {
        if (coordinator != null && coordinator.inRange(key)){
            KVUtils.printInfo("GET " + key + " from coordinator");
            return coordinator.get(key);
        }
        if (replica1 != null && replica1.inRange(key)){
            KVUtils.printInfo("GET " + key + " from replica1");
            return replica1.get(key);
        }
        if (replica2 != null && replica2.inRange(key)){
            KVUtils.printInfo("GET " + key + " from replica2");
            return replica2.get(key);
        }
        assert(false); // if key is not in range of storage, bug reported
        return null;
    }

    public void onServerShutDown() {
        if (coordinator != null) coordinator.onServerShutDown();
        if (replica1 != null) replica1.onServerShutDown();
        if (replica2 != null) replica2.onServerShutDown();
    }

    public void clearStorage() throws IOException {
        if (coordinator != null) coordinator.clearStorage();
        if (replica1 != null) replica1.clearStorage();
        if (replica2 != null) replica2.clearStorage();
    }

    public void transfer(List<KVServerCSocket> sockets, Boolean useCopy) {
        assert (coordinator != null);
        if (useCopy) coordinator.transfer(sockets.get(0));
        else coordinator.transfer(sockets);
    }

    public void duplicateTransferred(BigInteger keyFrom, BigInteger keyTo) {
        assert (coordinator != null);
        coordinator.copy(keyFrom, keyTo);
    }

    public void deleteDuplicate(){
        assert (coordinator != null);
        coordinator.deleteDuplicate();
    }

}
