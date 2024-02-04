package shared.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import constants.Constants;

public class KVMessageTool {    

    public static void sendMessage(KVMessage msg, OutputStream output) throws IOException {
        byte[] msgBytes = msg.getByteMessage();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
    }

    public static KVMessage receiveMessage(InputStream input) throws IOException {
        int idx = 0;

        byte[] msg = null, tmp = null;
        byte[] buffer = new byte[Constants.BUFFER_SIZE];
              
        /* read first char from stream */
        byte read = (byte) input.read();	
        boolean isReading = true;

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

    public static KVMessage parseMessage(String msg){
        try{
            msg = msg.substring(0, msg.length()-1); // remove '\r' at the end
            String[] tokens = msg.split("\\s+", 2);
                    
            StatusType type = parseStringType(tokens[0]);
            KVMessage message;

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
                    message = new KVMessage(type, tokens[1]);
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
            default:
                return StatusType.FAILED;
        }
    }

    public static String parseStatusType(StatusType type){
        String statusString = "";
        switch (type) {
            case GET:
                statusString = "GET";
                break;
            case GET_ERROR:
                statusString = "GET_ERROR";
                break;
            case GET_SUCCESS:
                statusString = "GET_SUCCESS";
                break;
            case PUT:
                statusString = "PUT";
                break;
            case PUT_SUCCESS:
                statusString = "PUT_SUCCESS";
                break;
            case PUT_UPDATE:
                statusString = "PUT_UPDATE";
                break;
            case PUT_ERROR:
                statusString = "PUT_ERROR";
                break;
            case DELETE_SUCCESS:
                statusString = "DELETE_SUCCESS";
                break;
            case DELETE_ERROR:
                statusString = "DELETE_ERROR";
                break;
            case INFO:
                statusString = "INFO";
                break;
            default:
                statusString = "FAILED";
                break;
        }
        return statusString;
    }
}
