package shared;

import org.apache.log4j.Logger;

public class Constants {

    public static final String LRU = "LRU";

    public static final String FIFO = "FIFO";

    public static final String LFU = "LFU";

    public static final String PATH_META = "/meta";
    public static final String PATH_DFILE = "/data";

    public static final String LOCAL_HOST = "localhost";

    public static String DELETE = "null";

    public static final String PATH_DCopy = "/copy";

    // size limit
    public static int MB = 1024 * 1024;

    public static int SST_SIZE = MB;

    // KVClient
    public static final String PROMPT = "KVClient> ";
    public static final int KEY_SIZE = 20;
    public static final int VAL_SIZE = 120;

    // KVStore
    public static final int PORT_LOW = 1024;
    public static final int PORT_HIGH = 65536;

    // KVMessage
    public static final int VALID_LOW = 31;
    public static final int VALID_HIGH = 127;
    public static final int SLASH_R = 13;
    public static final int SLASH_N = 10;
    public static final int ERROR = -1;
    public static final int BUFFER_SIZE = 1024;
    public static final int DROP_SIZE = 128 * BUFFER_SIZE;

    // KVServer
    public static final int CACHE_SIZE = 10;
    public static final String CACHE_STRATEGY = Constants.LRU;

    // KVServerStorage
    public static final Logger logger = Logger.getRootLogger();
    public static final String pwd = System.getProperty("user.dir");

    // ECSClient
    public static final String ECSPROMPT = "ECSClient> ";
}
