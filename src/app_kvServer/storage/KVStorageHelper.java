package app_kvServer.storage;

import app_kvServer.KVPair;
import app_kvServer.KVServer;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Objects;

import constants.Constants;

import static constants.Constants.DELETE;

/***
 * Logic:
 *        Write:
 *        new data is inserted into data file with level 0
 *        level 0 and level 1 have the same size limit
 *        from level 2, files are 2x in size of the previous level
 *        when a file reaches its limit, do a compaction to next level
 *        dirty data will be removed during compaction
 *        Read:
 *        when read, read from the smallest level, from the end of file
 */
public class KVStorageHelper {
    
    private DirMeta dirMeta;

    public KVStorageHelper() {
        String directory_path_str =  KVServer.dataPath;

        Path meta_path = Paths.get(directory_path_str + Constants.PATH_MATA);

        Constants.logger.error("path: " + meta_path);

        if (!Files.exists(meta_path)) {
            try {

                // create directory meta
                Files.createFile(meta_path);
                Files.write(meta_path, Collections.singletonList("0"));

                // create the first data file
                Path first_data_path = Paths.get(directory_path_str + Constants.PATH_DFile + 0);
                Files.createFile(first_data_path);
                Files.write(meta_path, Collections.singletonList("0,0,0"), StandardOpenOption.APPEND);

            } catch (IOException e) {
                Constants.logger.error("Failed to create empty data store.\n");
            }
        }

        // read directory meta
        try {
            this.dirMeta = new DirMeta(Files.readString(meta_path));
        } catch (IOException e) {
            Constants.logger.error("Failed to access storage.\n");
        }
    }

    public IKVMessage.StatusType put(KVPair pair) {

        // check if pair exists in storage, if it's not in cache
        IKVMessage.StatusType type;
        KVPair origin = get(pair.getKey());
        if (origin != null) {
            pair.setOriginLevel(origin.getOriginLevel());
            pair.setOriginLine(origin.getOriginLine());
            if (pair.getValue().equals(DELETE)) {
                if (origin.getValue().equals(DELETE)) {
                    return IKVMessage.StatusType.DELETE_ERROR;
                }
                type = IKVMessage.StatusType.DELETE_SUCCESS;
            } else {
                if (origin.getValue().equals(DELETE)) {
                    type = StatusType.PUT_SUCCESS;
                } else {
                    type = IKVMessage.StatusType.PUT_UPDATE;
                }
            }
        } else {
            if (pair.getValue().equals(DELETE)) {
                return StatusType.DELETE_ERROR;
            }
            type = IKVMessage.StatusType.PUT_SUCCESS;
        }

        // inform dirty in the original file
        if (pair.getOriginLevel() != -1) {
            DirMeta.FileMeta origin_meta = this.dirMeta.getFileMeta(pair.getOriginLevel());
            origin_meta.dirty_lines.add(pair.getOriginLine());
        }

        // write to level 0
        StringBuilder sb = new StringBuilder();
        sb
                .append(dirMeta.getFileMeta(0).num_pairs)
                .append(" ")
                .append(pair.getKey())
                .append(" ")
                .append(pair.getValue());

        Path path = Paths.get(KVServer.dataPath + Constants.PATH_DFile + 0);
        try {
            Files.write(path, Collections.singletonList(sb), StandardOpenOption.APPEND);
        } catch (IOException e) {
            Constants.logger.error("Unable to write to storage\n");

            if (origin == null) return IKVMessage.StatusType.PUT_ERROR;
            if (Objects.equals(pair.getValue(), DELETE)) return IKVMessage.StatusType.DELETE_ERROR;
            return IKVMessage.StatusType.PUT_ERROR;
        }

        DirMeta.FileMeta fMeta = dirMeta.getFileMeta(0);
        fMeta.size += sb.length();
        fMeta.num_pairs += 1;

        if (fMeta.size > Constants.MB) {
            compaction(0);
        }
        return type;
    }

    public KVPair get(String key) {

        int current_level = 0;

        while (current_level <= dirMeta.getLargest_level()) {

            String path_str = KVServer.dataPath + Constants.PATH_DFile + current_level;

            try (RandomAccessFile file = new RandomAccessFile(path_str, "r")){

                long fileSize = file.length() - 2;
                if (fileSize == -2) continue;
                StringBuilder sb = new StringBuilder();
                for (long pointer = fileSize; pointer >= 0; pointer--) {
                    file.seek(pointer);
                    char c = (char) file.read();

                    // one kv pair ended
                    if (c == '\n' && sb.length() > 0) {
                        KVPair pair = lineToKVPair(String.valueOf(sb.reverse()), current_level);
                        if (Objects.equals(pair.getKey(), key)) {
                            return pair;
                        }
                        sb = new StringBuilder();
                    } else {
                        sb.append(c);
                    }
                }
                KVPair pair = lineToKVPair(String.valueOf(sb.reverse()), current_level);
                if (Objects.equals(pair.getKey(), key)) {
                    return pair;
                }

            } catch (FileNotFoundException e) {

                // NO OPS, skip to next level

            } catch (IOException e) {
                Constants.logger.error("RandomAccessFile error.\n");
            } finally {
                current_level += 1;
            }

        }

        return null;

    }

    private void compaction(int level) {
        // TODO
    }

    private KVPair lineToKVPair(String line, int level) {
        String[] fields = line.split(" ");

        int originLine = Integer.parseInt(fields[0].trim());
        String key = fields[1].trim();
        String value = fields[2].trim();

        return new KVPair(key, value, false, level, originLine);
    }

    public void clearStorage() throws IOException {
        int current_level = 0;
        while (current_level <= dirMeta.getLargest_level()) {
            String path_str = KVServer.dataPath + Constants.PATH_DFile + current_level;
            Path path = Paths.get(path_str);
            Files.delete(path);

            current_level += 1;
        }
        Path meta = Paths.get(KVServer.dataPath + Constants.PATH_MATA);
        Files.delete(meta);

    }
}
