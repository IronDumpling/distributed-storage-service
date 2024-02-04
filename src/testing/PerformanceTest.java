package testing;

import org.junit.Test;

import cmnct_client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import java.util.ArrayList;

public class PerformanceTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
    @Test
    public void testPutGetRatio_8_2() {

        createPutsAndGets (80, 20);

    }

    // @Test
    // public void testPutGetRatio_5_5() {
    //     // Test with twice as many puts as gets
    //     createPutsAndGets(50, 50);
    // }

    // @Test
    // public void testPutGetRatio_2_8() {
    //     // Test with twice as many gets as puts
    //     createPutsAndGets(20, 80);
    // }

    public void createPutsAndGets(int numPuts, int numGets){
        Exception ex = null;
        long startTime = System.currentTimeMillis();
        IKVMessage respond;

        for (int i = 0; i < numPuts; i++) {
            String key = "key" + i;
            String value = "value" + i;
            try {
                respond = kvClient.put(key, value);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null);
        }

        for (int i = 0; i < numGets; i++) {
            String key = "key" + i;
            try {
                kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null);
        }

        long endTime = System.currentTimeMillis();
        long latency = endTime - startTime;
        double throughput = (double) (numPuts + numGets) / (latency / 1000.0);
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Latency: " + latency + " milliseconds");
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Throughput: " + throughput + " requests");
    }
	
	
}