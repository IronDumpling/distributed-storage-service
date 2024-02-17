package testing;

import org.junit.Test;

import cmnct_client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

        long totalTime = 0;
        int testingCounter = 50;
        int numPuts = 80;
        int numGets = 20;
        for(int i = 0 ; i < testingCounter; i++ ){
            totalTime = totalTime + createPutsAndGets (numPuts, numGets);
        }
        double throughput = (double) (80 + 20) / (totalTime / (1000.0 * testingCounter));
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Latency: " + totalTime/testingCounter + " milliseconds");
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Throughput: " + throughput + " requests");
        // TODO: Troughput should be measured in server side
    }

    @Test
    public void testPutGetRatio_5_5() {
        long totalTime = 0;
        int testingCounter = 50;
        int numPuts = 50;
        int numGets = 50;
        for(int i = 0 ; i < testingCounter; i++ ){
            totalTime = totalTime + createPutsAndGets (numPuts, numGets);
        }

        double throughput = (double) (numPuts + numGets) / (totalTime / (1000.0 * testingCounter));
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Latency: " + totalTime/testingCounter + " milliseconds");
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Throughput: " + throughput + " requests");

    }

    @Test
    public void testPutGetRatio_2_8() {
        // Test with twice as many gets as puts
        long totalTime = 0;
        int testingCounter = 50;
        int numPuts = 20;
        int numGets = 20;
        for(int i = 0 ; i < testingCounter; i++ ){
            totalTime = totalTime + createPutsAndGets (numPuts, numGets);
        }

        for(int i = 0 ; i < testingCounter; i++ ){
            totalTime = totalTime + createPutsAndGets (0, 20);
        }

        for(int i = 0 ; i < testingCounter; i++ ){
            totalTime = totalTime + createPutsAndGets (0, 20);
        }

        for(int i = 0 ; i < testingCounter; i++ ){
            totalTime = totalTime + createPutsAndGets (0, 20);
        }


        double throughput = (double) (numPuts + 80) / (totalTime / (1000.0 * testingCounter));
        System.out.println("ratio: " + numPuts +" to "+ 80 +" Latency: " + totalTime/testingCounter + " milliseconds");
        System.out.println("ratio: " + numPuts +" to "+ 80 +" Throughput: " + throughput + " requests");

    }

    public long createPutsAndGets(int numPuts, int numGets){
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
        return latency;
        
    }
	
	
}