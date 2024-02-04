package app_kvServer;

import shared.messages.IKVMessage.StatusType;

/* Deprecate this class, use KVMessage instead */
public class ServerException extends Exception{

    public StatusType status;

    public ServerException(StatusType status, String message) {
        super(message);
        this.status = status;
    }
}
