package app_kvECS;

import java.util.Map;

import ecs.ECSNode;
import ecs.IECSNode;

import java.util.ArrayList;
import java.util.Collection;

import java.net.Socket;

public interface IECSClient {
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start() throws Exception;

    /**
     * Stops all server instances and exits the remote processes.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean shutdown() throws Exception;

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     * @return  name of new server
     */
    public void joinServer(ECSNode node);

    /**
     * Removes nodes with names matching the nodeNames array
     * @param nodeNames names of nodes to remove
     * @return  true on success, false otherwise
     */
    public void removeServer(ECSNode node);

    /**
     * Get a map of all nodes
     */
    public ArrayList<ECSNode> getNodes();

    /**
     * Get the specific node responsible for the given key
     */
    public ECSNode getNodeByKey(String Key);
}
