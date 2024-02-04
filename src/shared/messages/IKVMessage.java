package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {
		GET, 			/* Get - <key> */
		GET_ERROR, 		/* requested tuple (i.e. value) not found - <key> */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found - <key> <value> */
		PUT, 			/* Put - <key> <value> */
		PUT_SUCCESS, 	/* Put - <key> <value> */
		PUT_UPDATE, 	/* Put - <key> <value> */
		PUT_ERROR, 		/* Put - <key> <value> */
		DELETE_SUCCESS, /* Delete - <key> <value> */
		DELETE_ERROR, 	/* Delete - <key> */
		FAILED,			/* failed to parse the request - <key> */
		INFO,			/* INFO - <key> */
		PUT_PENDING,    /* This should NEVER be returned - intermediate state when put not in cache */
		DELETE_PENDING  /* This should NEVER be returned - intermediate state when delete not in cache */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


