package app_kvServer.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class StorageMeta {

    public int nextIndex = 0;
    public int nextCopy = 0;
    public int version_number = 0;
    public int oldCount = 0;

    public HashMap<String, String> tableEntryMap = new HashMap<>();

    public StorageMeta(String content) {
        String[] lines = content.split("\n");
        String[] meta = lines[0].split(" ");

        this.version_number = Integer.parseInt(meta[0]);
        this.nextIndex = Integer.parseInt(meta[1]);
        this.nextCopy = Integer.parseInt(meta[2]);
        this.oldCount = Integer.parseInt(meta[3]);

        for (int i = 1; i < lines.length; i++) {
            String[] entry = lines[i].split(" ", 2);
            if (entry.length > 1) {
                tableEntryMap.put(entry[0], entry[1]);
            }
        }
    }

    public StorageMeta(){

    }

    public void WriteToMeta(Path path) throws IOException {

        List<String> lines = new ArrayList<>();
        lines.add(version_number + " " + nextIndex + " " + nextCopy + " " + oldCount);

        for (Map.Entry<String, String> entry : tableEntryMap.entrySet()) {
            lines.add(entry.getKey() + " " + entry.getValue());
        }

        Files.write(path, lines);
    }

}