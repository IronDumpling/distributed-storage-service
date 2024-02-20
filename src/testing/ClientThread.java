package testing;

import org.junit.Test;
import cmnct_client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ClientThread implements Runnable{
    private int NUM_PUTS;
    private int NUM_GETS;
    private List<Long> latencies;
    private KVStore kvClient;


    public ClientThread(List<Long> latencies, int numPuts, int numGets){
        NUM_GETS = numGets;
        NUM_PUTS = numPuts;
        this.latencies = latencies;

    }

    @Override
        public void run() {
            kvClient = new KVStore("localhost", 50000);
            try {
                kvClient.connect();
                long latency = createPutsAndGets(NUM_PUTS, NUM_GETS);
                synchronized (latencies) {
                    latencies.add(latency);
                }
            } catch (Exception e) {

            }finally {
                kvClient.disconnect();
            }
           
        }
    
    public long createPutsAndGets(int numPuts, int numGets){
        Exception ex = null;
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < numPuts; i++) {
            String key = "key" + i;
            String value = "value" + i;
            try {
                kvClient.put(key, value);
            } catch (Exception e) {
                ex = e;
            }
        }

        for (int i = 0; i < numGets; i++) {
            String key = "key" + i;
            try {
                kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }
        }

        long endTime = System.currentTimeMillis();
        long latency = endTime - startTime;
        return latency;
        
    }
}

