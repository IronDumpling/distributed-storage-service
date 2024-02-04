package testing;

import org.junit.Test;

import cmnct_client.KVStore;

import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.IOException;

public class AdditionalTest extends TestCase {
	
	/* Connection */
	// test 1. handle illegal port 2
	@Test
	public void testIllegalPort2() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 80);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	// test 2. connect twice
	public void testConnectTwice() {
		KVStore kvClient = new KVStore("localhost", 50000);
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.connect();
			kvClient.disconnect();
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	// test 3. disconnect first
	public void testDisconnectFirst() {
		KVStore kvClient = new KVStore("localhost", 50000);
		Exception ex = null;
		try {
			kvClient.disconnect();
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	// test 4. disconnect twice
	public void testDisconnectTwice() {
		KVStore kvClient = new KVStore("localhost", 50000);
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.disconnect();
			kvClient.disconnect();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	/* Interaction */
	// test 1. handle delete error
	public void testDeleteError() {
		String key = "deleteTestValue";
		String value = "toDelete";

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}
				
		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(key, "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);

		try {
			response = kvClient.put(key, value);
			kvClient.disconnect();
			response = kvClient.put(key, "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IOException);
	}

	// test 2. handle update the same
	public void testUpdateSame() {
		String key = "updateTestValue";
		String value = "toUpdate";

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}
				
		IKVMessage response = null;
		Exception ex = null;
		try {
			kvClient.put(key, value);
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
	}

	// test 3. handle update after disconnect
	public void testUpdateError() {		
		String key = "putTestValue";
		String value1 = "toPut";
		String value2 = "toUpdate";

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}
				
		IKVMessage response = null;
		Exception ex = null;
		try {
			kvClient.put(key, value1);
			kvClient.disconnect();
			response = kvClient.put(key, value2);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IOException);
	}

	// test 4. delete then get
	public void testDeleteGet() {
		String key = "deleteTestValue";
		String value = "toDelete";
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}
				
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);

		try {
			response = kvClient.put(key, "null");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	// test 5. storage
	public void testLargePutAndGet() {		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}

		int numPuts = 20, numGets = 20;
				
		IKVMessage response = null;
		Exception ex = null;

		for (int i = 0; i < numPuts; i++) {
            String key = "key" + i;
            String value = "value" + i;
            try {
                response = kvClient.put(key, value);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
        }

		for (int i = 0; i < numGets; i++) {
            String key = "key" + i;
            try {
                response = kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
        }
	}

	// test 6. Large Update and Delete
	public void testLargeUpdateAndDelete() {		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {}

		int numPuts = 40;
				
		IKVMessage response = null;
		Exception ex = null;
		for (int i = 0; i < numPuts; i++) {
			String key = "KEY" + i;
			String value = "value" + i;
			try {
				response = kvClient.put(key, value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		}

		for (int i = 0; i < numPuts/2; i++) {
			String key = "KEY" + i;
			String value = "VALUE" + i;
			try {
				response = kvClient.put(key, value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
		}

		for (int i = numPuts/2; i < numPuts; i++) {
			String key = "KEY" + i;
			String value = "null";
			try {
				response = kvClient.put(key, value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
		}

		for (int i = 0; i < numPuts/2; i++) {
			String key = "KEY" + i;
			try {
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}

		for (int i = numPuts/2; i < numPuts; i++) {
			String key = "KEY" + i;
			try {
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
		}
	}
}
