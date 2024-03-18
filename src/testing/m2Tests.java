package testing;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import cmnct_client.KVStore;

public class m2Tests extends TestCase {
    private static final String JAR_PATH = "./m2-server.jar";
    private static final String LOG_FOLDER = "./logs/testing/";
    private static final String[] ECS_COMMAND = {
        "java", "-jar", "../../m2-ecs.jar", "-p", "50030"
    };

    Process ecs = null;
    Process[] server = new Process[10];

    @Before
    public void setUp() throws IOException {
        // Create the log folder if it doesn't exist
        Files.createDirectories(Paths.get(LOG_FOLDER));
    }

    @After
    public void tearDown() {
        if (ecs != null) {
            ecs.destroy();
        }
        
        for(Process s : server){
            s.destroy();
        }
    }
    
    /* Consistent Hashing */


    /* Meta Data Updates */

    /* Retry */
    @Test
    public void testThreeServers() {
        KVStore kvClient = new KVStore("localhost", 50050);

        ProcessBuilder ecsBuilder = new ProcessBuilder(ECS_COMMAND);

        ProcessBuilder[] serverBuilders = new ProcessBuilder[3];
        for(int i = 0; i < serverBuilders.length; i++){
            String[] command = {"java", "-jar", JAR_PATH, 
                                "-b", "127.0.0.1:50030", 
                                "-d", "./data", "-p", "5005" + i};
            serverBuilders[i] = new ProcessBuilder(command);
            Path outputLogFile = Paths.get(LOG_FOLDER, "server"+i+"_stdout.log");
            Path errorLogFile = Paths.get(LOG_FOLDER, "server"+i+"_stderr.log");
            serverBuilders[i].redirectOutput(outputLogFile.toFile());
            serverBuilders[i].redirectError(errorLogFile.toFile());
        }

        Exception ex = null;
        IKVMessage[] response = new IKVMessage[4];
        

        try {
            ecs = ecsBuilder.start();
            server[0] = serverBuilders[0].start();

            kvClient.connect();
            kvClient.put("apple_1", "100");
            kvClient.put("banana2", "110");
            kvClient.put("peach@3", "120");
            kvClient.put("grape4!", "130");

            response[0] = kvClient.get("apple_1");
            response[1] = kvClient.get("banana2");
            response[2] = kvClient.get("peach@3");
            response[3] = kvClient.get("grape4!");
        } catch (IOException e){
            ex = e;
        } catch (Exception e){
            ex = e;
        }

        assertTrue(ex == null || 
                response[0].getStatus() == StatusType.GET_SUCCESS ||
                response[1].getStatus() == StatusType.GET_SUCCESS ||
                response[2].getStatus() == StatusType.GET_SUCCESS ||
                response[3].getStatus() == StatusType.GET_SUCCESS);

        try {
            server[1] = serverBuilders[1].start();
            response[0] = kvClient.get("apple_1");
            response[1] = kvClient.get("banana2");
            response[2] = kvClient.get("peach@3");
            response[3] = kvClient.get("grape4!");
        } catch (IOException e){
            ex = e;
        } catch (Exception e){
            ex = e;
        }

        assertTrue(ex == null || 
        response[0].getStatus() == StatusType.GET_SUCCESS ||
        response[1].getStatus() == StatusType.GET_SUCCESS ||
        response[2].getStatus() == StatusType.GET_SUCCESS ||
        response[3].getStatus() == StatusType.GET_SUCCESS);

        try {
            server[2] = serverBuilders[2].start();
            response[0] = kvClient.get("apple_1");
            response[1] = kvClient.get("banana2");
            response[2] = kvClient.get("peach@3");
            response[3] = kvClient.get("grape4!");
            kvClient.disconnect();
        } catch (IOException e){
            ex = e;
        } catch (Exception e){
            ex = e;
        }
        
        assertTrue(ex == null || 
        response[0].getStatus() == StatusType.GET_SUCCESS ||
        response[1].getStatus() == StatusType.GET_SUCCESS ||
        response[2].getStatus() == StatusType.GET_SUCCESS ||
        response[3].getStatus() == StatusType.GET_SUCCESS);
    }

}
