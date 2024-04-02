package app_kvECS;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.text.NumberFormat;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvECS.ECSCmnct;

import shared.KVUtils;
import shared.Constants;
import shared.KVMeta;

public class ECSClient {

    private static int port = 0;
    private static String address = Constants.LOCAL_HOST_IP;
    public static String dataPath = Constants.PATH_DFILE;
    private static String logPath = "logs/ecs.log";
    private static Logger logger = Logger.getRootLogger();
    private static Level logLevel = Level.ALL;

    public static void main(String[] args) {
        boolean invalidInput = false;
        boolean hasPort = false;
              
        try {		
            new LogSetup("logs/ecs.log", logLevel);
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-a":
                        if (i + 1 < args.length) {
                            address = args[i + 1];
                            i++;  
                        } else {
                            KVUtils.printError("Missing value for -a", null, logger);
                            invalidInput = true;
                        }
                        break;
                    case "-p":
                        if (i + 1 < args.length) {
                            port = Integer.parseInt(args[i + 1]);
                            i++; 
                            hasPort = true;
                        } else {
                            KVUtils.printError("Missing value for -p", null, logger);
                            invalidInput = true;
                        }
                        break;
                    case "-l":
                        if (i + 1 < args.length) {
                            logPath = args[i + 1];
                            i++;  
                        } else {
                            KVUtils.printError("Missing value for -l", null, logger);
                            invalidInput = true;
                        }
                        break;
                    case "-ll":
                        if (i + 1 < args.length) {
                            logLevel = KVUtils.getLogLevel(args[i + 1].toUpperCase());
                            i++;  
                        } else {
                            KVUtils.printError("Missing value for -ll", null, logger);
                            invalidInput = true;
                        }
                        break;
                    default:
                        KVUtils.printError("Unknown option: " + args[i], null, logger);
                        invalidInput = true;
                }
            }
            if(invalidInput) {
                KVUtils.printError("Invalid number of arguments!", null, logger);
                KVUtils.printInfo("Usage: ECS -p <port number> -a <address> -l <logPath> -ll <logLevel>!", logger);
            } else if(!hasPort){
                KVUtils.printError("Invalid number of arguments!", null, logger);
                KVUtils.printInfo("Usage: ECS -p <port number> -a <address> -l <logPath> -ll <logLevel>!", logger);
            } else {
                new ECSClient(port).run();
            }
        } catch (IOException ioe) {
            KVUtils.printError("Unable to initialize logger!", ioe, logger);
            ioe.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            KVUtils.printError("Invalid argument <port>! Not a number!", nfe, logger);
            KVUtils.printInfo("Usage: ECS -p <port number> -a <address> -l <logPath> -ll <logLevel>!", logger);
            System.exit(1);
        }
    }

    private ECSCmnct ecsCmnct;
    private boolean isRunning = true;
    private BufferedReader stdin;

    /* Life Cycle */
    public ECSClient(int port) {
        this.ecsCmnct = new ECSCmnct(port);
        new Thread(this.ecsCmnct).start();
    }

    public void run(){
        
        while(isRunning) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(Constants.ECSPROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException ioe) {
                isRunning = false;
                KVUtils.printError("CLI does not respond - Application terminated", ioe, logger);
            } catch (Exception e) {
                isRunning = false;
                KVUtils.printError("Unexpected things happen when reading commands", e, logger);
            }
        }
    }

    private void handleCommand(String cmdLine) throws IOException{
        String[] tokens = cmdLine.split("\\s+");

        switch (tokens[0]) {
            case "remove_server":
                ecsCmnct.inputRemoveServer(tokens[1]);
                break;
            case "add_server":
                // TODO Add server
                break;
            case "quit":
                stop();
                break;
            case "help":
                help();
                break;
            case "print":
                ecsCmnct.printECS();
                ecsCmnct.getMeta().printKVMeta();
                break;
            default:
                commandError(tokens[0]);
        }
    }

    public void stop() {
        isRunning = false;
    }

    private void help(){
        StringBuilder sb = new StringBuilder();
        sb.append("\nECS-Client HELP (Usage):\n");
        sb.append("\n================================");
        sb.append("================================\n");
        
        sb.append("remove_server <IP>:<Port>");
        sb.append("\t Remove specific server from the distributed sotrage system.\n");
        sb.append("");

        sb.append("add_server <IP>:<Port>");
        sb.append("\t\t Add specific server to the distributed sotrage system.\n");
        sb.append("");

        sb.append("print");
        sb.append("\t\t Print information of the ECS.\n");
        sb.append("");
        
        sb.append("quit");
        sb.append("\t\t\t\t Exits the program. \n");
        sb.append("\n================================");
        sb.append("================================\n");
        
        System.out.println(sb.toString());
    }

    private void commandError(String cmd){
        switch (cmd.toLowerCase()){
            case "":
                break;
            default:
                KVUtils.printError("Unknown command!", null, logger);
                help();
        }
    }
}
