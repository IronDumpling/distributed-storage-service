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
import java.util.List;

public class PerformanceTest extends TestCase {

	private KVStore kvClient;

	
	public void setUp() {
		// kvClient = new KVStore("localhost", 50000);
		// try {
		// 	kvClient.connect();
		// } catch (Exception e) {}
	}

	// public void tearDown() {
    //     kvClient.disconnect();

	// }
	
    @Test
    public void testPutGetRatio_8_2() {
        int numPuts = 80;
        int numGets = 20;
        int numClient = 10;
        multiClientTest(numClient, numPuts, numGets);
    }

    

    @Test
    public void testPutGetRatio_5_5() {
        int numPuts = 50;
        int numGets = 50;
        int numClient = 10;
        multiClientTest(numClient, numPuts, numGets);
    }

    @Test
    public void testPutGetRatio_2_8() {
        int numPuts = 20;
        int numGets = 80;
        int numClient = 10;
        multiClientTest(numClient, numPuts, numGets);
    }

	public void multiClientTest (int numClient, int numPuts, int numGets){
        List<Thread> threads = new ArrayList<>();
        List<Long> latencies = new ArrayList<>();
        long totalTime = 0;
  
        for (int i = 0; i < numClient; i++) {
            ClientThread clientThread = new ClientThread(latencies, numPuts, numGets);
            Thread thread = new Thread(clientThread);
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Calculate average latency
        
        for (long latency : latencies) {
            totalTime += latency;
        }
        double averageLatency = (double) totalTime / numClient;

        //System.out.println("Average Latency across all clients: " + averageLatency + " milliseconds");
        double throughput = (double) (80 + 20) / (totalTime / (1000.0 * numClient));
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Latency: " + averageLatency + " milliseconds");
        System.out.println("ratio: " + numPuts +" to "+ numGets +" Throughput: " + throughput + " requests");
    }
	
}