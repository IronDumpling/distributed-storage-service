package app_kvServer.storage;

import java.util.ArrayList;
import java.util.HashMap;

public class DirMeta {

    public class FileMeta {

        int num_pairs;
        int size;
        ArrayList<Integer> dirty_lines;

        public FileMeta(int num_pairs, int size, ArrayList<Integer> dirty_lines) {
            this.num_pairs = num_pairs;
            this.size = size;
            this.dirty_lines = dirty_lines;
        }

    }

    private int largest_level;

    private final HashMap<Integer, FileMeta> fileMetas;

    public DirMeta(String content) {
        String[] lines = content.split("\n");
        this.largest_level = Integer.parseInt(lines[0]);
        this.fileMetas = new HashMap<>();

        if (lines.length == 1) return;

        for (int i = 1; i < lines.length; i++) {
            String[] fields = lines[i].split(",");

            int num_pairs = Integer.parseInt(fields[0]);
            int index = Integer.parseInt(fields[1]);
            int size = Integer.parseInt(fields[2]);

            ArrayList<Integer> dirty_lines = new ArrayList<>();
            if (num_pairs > 0) {
                for (int j = 3; j < fields.length; j++) {
                    dirty_lines.add(Integer.parseInt(fields[j]));
                }
            }

            fileMetas.put(index, new FileMeta(num_pairs, size, dirty_lines));
        }
    }

    public int getLargest_level() {
        return largest_level;
    }

    public void setLargest_level(int largest_level) {
        this.largest_level = largest_level;
    }

    public FileMeta getFileMeta(int level) {
        return fileMetas.get(level);
    }
}
