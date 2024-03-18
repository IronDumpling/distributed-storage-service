package app_kvServer.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class StorageMeta {

    public int nextIndex = 0;
    public int nextCopy = 0;
    public int version_number = 0;
    public int oldCount = 0;

    public StorageMeta(String content) {
        String[] meta = content.split(" ");

        this.version_number = Integer.parseInt(meta[0]);
        this.nextIndex = Integer.parseInt(meta[1]);
        this.nextCopy = Integer.parseInt(meta[2]);
        this.oldCount = Integer.parseInt(meta[3]);
    }

    public StorageMeta(){

    }

    public void WriteToMeta(Path path) throws IOException {
        Files.write(path, Collections.singletonList(
                        version_number + " " + nextIndex + " " + nextCopy + " " + oldCount));
    }

}
