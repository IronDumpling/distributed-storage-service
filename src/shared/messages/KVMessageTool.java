package shared.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;

import java.util.List;
import java.util.ArrayList;

import shared.KVPair;
import shared.KVSubscribe;
import shared.KVUtils;
import shared.Constants;
import shared.KVMeta;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

public class KVMessageTool {    

    public static void sendMessage(IKVMessage msg, OutputStream output) throws IOException {
        byte[] msgBytes = msg.getByteMessage();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
    }

    public static IKVMessage receiveMessage(InputStream input) throws IOException {
        int idx = 0;

        byte[] msg = null, tmp = null;
        byte[] buffer = new byte[Constants.BUFFER_SIZE];
              
        /* read first char from stream */
        byte read = (byte) input.read();	
        boolean isReading = true;
        if( read == Constants.ERROR || read == Constants.SLASH_N || 
            read == Constants.SLASH_R){
            return null;
        }

        while(read != Constants.ERROR && isReading) {
            if(read == Constants.SLASH_N && 
            (idx > 0 && buffer[idx-1] == Constants.SLASH_R) ||
            (idx == 0 && msg != null && msg[msg.length-1] == Constants.SLASH_R)){
                isReading = false;
                continue;
            }
            
            // buffer limit reached
            if(idx == Constants.BUFFER_SIZE){
                if(msg == null){
                    tmp = new byte[Constants.BUFFER_SIZE];
                    System.arraycopy(buffer, 0, tmp, 0, Constants.BUFFER_SIZE);
                } else {
                    tmp = new byte[msg.length + Constants.BUFFER_SIZE];
                    System.arraycopy(msg, 0, tmp, 0, msg.length);
                    System.arraycopy(buffer, 0, tmp, msg.length, Constants.BUFFER_SIZE);
                }
                msg = tmp;
                buffer = new byte[Constants.BUFFER_SIZE];
                idx = 0;
            }

            // drop limit reached
            if(msg != null && msg.length + idx >= Constants.DROP_SIZE){
                isReading = false;
                continue;
            }

            // read valid characters
            if(read > Constants.VALID_LOW && read < Constants.VALID_HIGH || read == Constants.SLASH_R){
                buffer[idx] = read;
                idx++;
            }
                        
            // read next character from stream
            read = (byte) input.read();
        }

        if(msg == null){
            tmp = new byte[idx];
            System.arraycopy(buffer, 0, tmp, 0, idx);
        } else {
            tmp = new byte[msg.length + idx];
            System.arraycopy(msg, 0, tmp, 0, msg.length);
            System.arraycopy(buffer, 0, tmp, msg.length, idx);
        }
        msg = tmp;

        return parseMessage(new String(msg, StandardCharsets.UTF_8));
    }

    public static IKVMessage parseMessage(String msg){
        try{
            IKVMessage message;
            if(msg.length() <1){
                message = new KVMessage(parseStringType("FAILED"));
            }
            msg = msg.substring(0, msg.length()-1); // remove '\r' at the end
            String[] tokens = msg.split("\\s+", 2);
                    
            StatusType type = parseStringType(tokens[0]);
            
            switch (type) {
                case GET:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case GET_ERROR:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case INFO:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case FAILED:
                    message = new KVMessage(type);
                    break;
                case META:
                    message = new KVMessage(type);
                    break;
                case META_UPDATE:
                    message = new KVMeta(type, tokens[1]);
                    break;
                case SERVER_NOT_RESPONSIBLE:
                    message = new KVMessage(type);
                    break;
                case SERVER_WRITE_LOCK:
                    message = new KVMessage(type);
                    break;
                case SERVER_STOPPED:
                    message = new KVMessage(type);
                    break;
                case SERVER_ACTIVE:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case SERVER_REMOVE:
                    message = new KVMessage(type);
                    break;
                case DATA_TRANSFER:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case DATA_TRANSFER_SUCCESS:   
                    message = new KVMessage(type);
                    break;
                case DATA_TRANSFER_SINGLE:
                    String[] keyvalue = tokens[1].split("\\s+", 3);
                    message = new KVPair(keyvalue[0], keyvalue[1], keyvalue[2], false);
                    break;
                case DATA_TRANSFER_TABLE:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case SELECT_SUCCESS:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case SELECT_FAIL:
                    message = new KVMessage(type);
                    break;
                case SUBSCRIBE_UPDATE:
                    message = new KVSubscribe(tokens[1]);
                    break;
                case SUBSCRIBE_SUCCESS:
                    message = new KVMessage(type);
                    break;
                case SUBSCRIBE_FAIL:
                    message = new KVMessage(type);
                    break;
                case UNSUBSCRIBE_SUCCESS:
                    message = new KVMessage(type);
                    break;
                case UNSUBSCRIBE_FAIL:
                    message = new KVMessage(type);
                    break;
                case PUT_TABLE_SUCCESS:
                    message = new KVMessage(type);
                    break;
                case PUT_TABLE_FAIL:
                    message = new KVMessage(type);
                    break;
                case CREATE_TABLE_SUCCESS:
                    message = new KVMessage(type);
                    break;
                case CREATE_TABLE_FAIL:
                    message = new KVMessage(type);
                    break;
                case DESTROY_TABLE:
                    message = new KVMessage(type, tokens[1]);
                    break;
                case DESTROY_TABLE_SUCCESS:
                    message = new KVMessage(type);
                    break;
                case DESTROY_TABLE_FAIL:
                    message = new KVMessage(type);
                    break;
                default:
                    String[] pair = tokens[1].split("\\s+", 2);
                    message = new KVMessage(type, pair[0], pair[1]);
                    break;
            }
            return message;
        } catch (StringIndexOutOfBoundsException e) {
            Constants.logger.error("Access string out of bound!", e);
            return null;
        } catch (Exception e) {
            Constants.logger.error("Unexpected things happen when parsing messages", e);
            return null;
        }
    }

    public static StatusType parseStringType(String type){
        switch (type.toUpperCase()) {
            case "GET":
                return StatusType.GET;
            case "GET_ERROR":
                return StatusType.GET_ERROR;
            case "GET_SUCCESS":
                return StatusType.GET_SUCCESS;
            case "PUT":
                return StatusType.PUT;
            case "PUT_SUCCESS":
                return StatusType.PUT_SUCCESS;
            case "PUT_UPDATE":
                return StatusType.PUT_UPDATE;
            case "PUT_ERROR":
                return StatusType.PUT_ERROR;
            case "DELETE_SUCCESS":
                return StatusType.DELETE_SUCCESS;
            case "DELETE_ERROR":
                return StatusType.DELETE_ERROR;
            case "INFO":
                return StatusType.INFO;
            case "META":
                return StatusType.META;
            case "META_UPDATE":
                return StatusType.META_UPDATE;
            case "SERVER_NOT_RESPONSIBLE":
                return StatusType.SERVER_NOT_RESPONSIBLE;
            case "SERVER_WRITE_LOCK":
                return StatusType.SERVER_WRITE_LOCK;
            case "SERVER_STOPPED":
                return StatusType.SERVER_STOPPED;
            case "SERVER_ACTIVE":
                return StatusType.SERVER_ACTIVE;
            case "DATA_TRANSFER":
                return StatusType.DATA_TRANSFER;
            case "DATA_TRANSFER_SINGLE":
                return StatusType.DATA_TRANSFER_SINGLE;
            case "DATA_TRANSFER_SUCCESS":
                return StatusType.DATA_TRANSFER_SUCCESS;
            case "DATA_TRANSFER_TABLE":
                return StatusType.DATA_TRANSFER_TABLE;
            case "SERVER_REMOVE":
                return StatusType.SERVER_REMOVE;
            case "KEYRANGE_SUCCESS":
                return StatusType.META_UPDATE;
            case "SELECT":
                return StatusType.SELECT;
            case "SELECT_SUCCESS":
                return StatusType.SELECT_SUCCESS;
            case "SELECT_FAIL":
                return StatusType.SELECT_FAIL;
            case "SUBSCRIBE":
                return StatusType.SUBSCRIBE;
            case "SUBSCRIBE_SUCCESS":
                return StatusType.SUBSCRIBE_SUCCESS;
            case "SUBSCRIBE_FAIL":
                return StatusType.SUBSCRIBE_FAIL;
            case "SUBSCRIBE_EVENT":
                return StatusType.SUBSCRIBE_EVENT;
            case "SUBSCRIBE_UPDATE":
                return StatusType.SUBSCRIBE_UPDATE;
            case "UNSUBSCRIBE":
                return StatusType.UNSUBSCRIBE;
            case "UNSUBSCRIBE_SUCCESS":
                return StatusType.UNSUBSCRIBE_SUCCESS;
            case "UNSUBSCRIBE_FAIL":
                return StatusType.UNSUBSCRIBE_FAIL;
            case "PUT_TABLE":
                return StatusType.PUT_TABLE;
            case "PUT_TABLE_SUCCESS":
                return StatusType.PUT_TABLE_SUCCESS;
            case "PUT_TABLE_FAIL":
                return StatusType.PUT_TABLE_FAIL;
            case "CREATE_TABLE":
                return StatusType.CREATE_TABLE;
            case "CREATE_TABLE_SUCCESS":
                return StatusType.CREATE_TABLE_SUCCESS;
            case "CREATE_TABLE_FAIL":
                return StatusType.CREATE_TABLE_FAIL;
            case "DESTROY_TABLE":
                return StatusType.DESTROY_TABLE;
            case "DESTROY_TABLE_SUCCESS":
                return StatusType.DESTROY_TABLE_SUCCESS;
            case "DESTROY_TABLE_FAIL":
                return StatusType.DESTROY_TABLE_FAIL;
            default:
                return StatusType.FAILED;
        }
    }

    public static List<KVPair> convertToKVPairList(IKVMessage message){
        String msg = message.getMessage().trim();
        String[] tokens = msg.split("\\s+", 2);
        List<KVPair> transferData = new ArrayList<>();
        String table;
        String key;
        String value;
        if (tokens.length < 2) return transferData;
        for(String pair: tokens[1].split("<DELIMITER>")){
            String[] keyValue = pair.split(" ", 3);
            if (pair.isEmpty()) continue;
            table = keyValue[0];
            key = keyValue[1];
            value = keyValue[2];
            transferData.add(new KVPair(table, key, value, null));
        }
        return transferData;
    }

    public static List<KVPair> convertToTableList(IKVMessage message) {
        // NOTE: The KVPair here is not the normal KVPair
        // I use KVPair because it's easy to support a key, value structure
        String msg = message.getMessage().trim();
        String[] tokens = msg.split("\\s+", 2);
        List<KVPair> transferData = new ArrayList<>();
        String key;
        String value;
        if (tokens.length < 2) return transferData;
        for(String pair: tokens[1].split("<DELIMITER>")){
            String[] keyValue = pair.split(" ", 2);
            if (pair.isEmpty()) continue;
            key = keyValue[0];
            value = keyValue[1];
            transferData.add(new KVPair(key, value, null));
        }
        return transferData;
    }
}
