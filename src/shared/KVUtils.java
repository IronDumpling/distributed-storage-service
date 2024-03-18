package shared;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.math.BigInteger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import app_kvServer.KVServer.ServerStatus;
import shared.messages.IKVMessage.StatusType;

public class KVUtils {
    public static Level getLogLevel(String logLevel){
        Level level = Level.ALL;
        boolean success = true;

        switch (logLevel.toUpperCase()) {
            case "ALL":
                level = Level.ALL;
                break;
            case "DEBUG":
                level = Level.DEBUG;
                break;
            case "INFO":
                level = Level.INFO;
                break;
            case "WARN":
                level = Level.WARN;
                break;
            case "ERROR":
                level = Level.ERROR;
                break;
            case "FATAL":
                level = Level.FATAL;
                break;
            case "OFF":
                level = Level.OFF;
                break;
            default:
                printError("Unknown log level!", null, null);
                System.out.println("\t  Possible log levels are:");
                System.out.println("\t  ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
        }
        
        if(success) printSuccess("Current log level is: " + logLevel, null);

        return level;
    }

    public static void printError(String msg, Exception e, Logger logger){
        System.out.println("[ERROR]: " + msg);
        if(e != null && logger != null) logger.error(msg, e);
    }
    
    public static void printSuccess(String msg, Logger logger){
        System.out.println("[SUCCESS]: " + msg);
        if(logger != null) logger.info(msg);
    }
    public static void printInfo(String msg, Logger logger){
        System.out.println(msg);
        if(logger != null) logger.info(msg);
    }

    /* Consistent Hashing */
    public static BigInteger consistHash(String input){
        BigInteger hashValue = null;
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(input.getBytes());
            
            StringBuilder hexString = new StringBuilder();
            for (byte hashByte : hashBytes) {
                String hex = Integer.toHexString(0xFF & hashByte);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            hashValue = new BigInteger(hexString.toString(), 16);
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            printError("Not found MD5 algorithm", e, null);
        }
        return hashValue;
    }

    /* Server Status */
    public static StatusType transferSeverStatusToStatusType(ServerStatus status){
        StatusType result = StatusType.SERVER_STOPPED;
        switch(status){
            case ACTIVATED:
                result = StatusType.SERVER_ACTIVE;
                break;
            case IN_TRANSFER:
                result = StatusType.SERVER_WRITE_LOCK;
                break;
            case STOPPED:
                result = StatusType.SERVER_STOPPED;
                break;
            case REMOVE:
                result = StatusType.SERVER_REMOVE;
                break;
        }
        return result;
    }


    public static ServerStatus parseStringToServerStatus(String str) {
        ServerStatus status = null;
        switch(str){
            case "ACTIVATED":
                status = ServerStatus.ACTIVATED;
                break;
            case "IN_TRANSFER":
                status = ServerStatus.IN_TRANSFER;
                break;
            case "STOPPED":
                status = ServerStatus.STOPPED;
                break;
            case "REMOVE":
                status = ServerStatus.REMOVE;
                break;
            default:
                printError("Server status " + str + "is invalid!", null, null);
        }
        return status;
    }
}
