package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {
		INFO,					/* Human read or dummy information - <key> */
		FAILED,					/* Failed to parse the request - <key> */

		GET, 					/* Get - <key> */
		GET_ERROR, 				/* Requested key not found - <key> */
		GET_SUCCESS, 			/* Requested key found - <key> <value> */
		PUT, 					/* Put request - <key> <value> */
		PUT_SUCCESS, 			/* Put success- <key> <value> */
		PUT_UPDATE, 			/* Update existing key - <key> <value> */
		PUT_ERROR, 				/* Put error - <key> <value> */
		DELETE_SUCCESS,			/* Delete key found - <key> <value> */
		DELETE_ERROR, 			/* Delete key not found - <key> */
		SERVER_NOT_RESPONSIBLE,	/* Server responds the requested key is out of range - <key> */
		
		PENDING,				/* Server still needs to check the key in the storage - <none> */
		
		META,					/* Client requests META_DATA from server - <none> */
		META_UPDATE,			/* Update META_DATA - <value> */

		SERVER_WRITE_LOCK,		/* Server responds its status is WRITE_LOCK - <none> */
		SERVER_STOPPED,			/* Server responds its status is STOPPED - <none> */
		SERVER_ACTIVE,          /* Server responds its status is ACTIVE - <none> */
		SERVER_REMOVE,			/* server responds its status is deleted */

		DATA_TRANSFER,			/* Server send rebalancing data to another server - <value> */
		DATA_TRANSFER_SUCCESS,	/* Server confirm completed to sender server - <none> */

		DATA_REPLICATE_SINGLE,  /* Server send a single kv pair as replicate <key> <value> */

		 						/* Try to shutdown the server - <none> */ 
	}

	public String getKey();

	public String getValue();

	public StatusType getStatus();

	public String getStringStatus();

	public String getMessage();

	public byte[] getByteMessage();
}


