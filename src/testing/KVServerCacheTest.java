package testing;

import app_kvServer.KVServer;
import app_kvServer.ServerException;
import shared.Constants;
import junit.framework.TestCase;

import org.junit.Test;

public class KVServerCacheTest extends TestCase {

    @Test
    public void testFIFO() {
        KVServer server = new KVServer(80, 10, Constants.FIFO);

        try {
            for (int i = 0; i < 100; i++) {
                server.putKV(String.valueOf(i), String.valueOf(i * 2));
            }
        } catch (ServerException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 90; i++) {
            assertFalse(server.inCache(String.valueOf(i)));
            assertTrue(server.inStorage(String.valueOf(i)));
        }
        for (int i = 90; i < 100; i++) {
            assertTrue(server.inCache(String.valueOf(i)));
            assertFalse(server.inStorage(String.valueOf(i)));
        }
        server.clearStorage();
    }

    @Test
    public void testLRU() {
        KVServer server = new KVServer(80, 5, Constants.LRU);

        try {
            server.putKV("1", "1"); // cache: 1
            server.putKV("2", "1"); // cache: 2 1
            server.putKV("3", "1"); // cache: 3 2 1
            server.putKV("1", "1"); // cache: 1 3 2
            server.putKV("4", "1"); // cache: 4 1 3 2
            server.putKV("5", "1"); // cache: 5 4 1 3 2
            server.putKV("6", "1"); // cache: 6 5 4 1 3
            assertFalse(server.inCache("2"));
            server.putKV("4", "1"); // cache: 4 6 5 1 3
            assertTrue(server.inCache("3"));
            
            // to test mass insertion succeeded
            for (int i = 0; i < 100; i++) {
                server.putKV(String.valueOf(i), String.valueOf(i * 2));
            }
        } catch (ServerException e) {
            throw new RuntimeException(e);
        }
        server.clearStorage();
    }

    @Test
    public void testLFU() {

        KVServer server = new KVServer(80, 5, Constants.LFU);

        try {

            server.putKV("1", "1"); // cache: 1
            server.putKV("2", "1"); // cache: 2 1
            server.putKV("3", "1"); // cache: 3 2 1
            server.putKV("4", "1"); // cache: 4 3 2 1
            server.putKV("5", "1"); // cache: 5 4 3 2 1
            server.putKV("6", "1"); // cache: 6 5 4 3 2
            assertFalse(server.inCache("1"));

            server.putKV("2", "1"); // cache: 2 6 5 4 3
            server.putKV("7", "1"); // cache: 2 7 6 5 4
            assertFalse(server.inCache("3"));

            // loop over the current 5 values in cache
            server.putKV("7", "1");
            server.putKV("2", "1");
            server.putKV("6", "1");
            server.putKV("5", "1");
            server.putKV("4", "1"); // cache: 2 4 5 6 7

            
            server.putKV("8", "1"); // cache: 2 4 5 6 8
            assertFalse(server.inCache("7"));
            server.putKV("9", "1"); // cache: 2 4 5 6 9 

            // insert 4 new values twice
            server.putKV("80", "1");
            server.putKV("80", "1");
            server.putKV("81", "1");
            server.putKV("81", "1");
            server.putKV("82", "1");
            server.putKV("82", "1");
            server.putKV("83", "1");
            server.putKV("83", "1");

            assertTrue(server.inCache("2"));
            assertTrue(server.inCache("80"));
            assertTrue(server.inCache("81"));
            assertTrue(server.inCache("82"));
            assertTrue(server.inCache("83"));

             // to test mass insertion succeeded
            for (int i = 0; i < 100; i++) {
                server.putKV(String.valueOf(i), String.valueOf(i * 2));
            }

        } catch (ServerException e) {
            throw new RuntimeException(e);
        }
        server.clearStorage();
    }
}
