package org.I0Itec.zkclient;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

public class DistributedQueue<T extends Serializable> {

    private ZkClient _zkClient;
    private String _root;

    private static final String ELEMENT_NAME = "element";

    public DistributedQueue(ZkClient zkClient, String root) {
        _zkClient = zkClient;
        _root = root;
    }

    public boolean offer(T element) throws KeeperException, InterruptedException, IOException {
        _zkClient.create(_root + "/" + ELEMENT_NAME + "-", element, CreateMode.PERSISTENT_SEQUENTIAL);
        return true;
    }

    @SuppressWarnings("unchecked")
    public T poll() throws KeeperException, InterruptedException, IOException {
        while (true) {
            List<String> list = _zkClient.getChildren(_root, true);
            if (list.size() == 0) {
                return null;
            }
            String elementName = getSmallestElement(list);

            try {
                T data = (T) _zkClient.readData(_root + "/" + elementName);
                _zkClient.delete(_root + "/" + elementName);
                return data;
            } catch (NoNodeException e) {
                // somebody picked up the element first
            }
        }
    }

    private String getSmallestElement(List<String> list) {
        String smallestElement = list.get(0);
        for (String element : list) {
            if (element.compareTo(smallestElement) < 0) {
                smallestElement = element;
            }
        }
        
        return smallestElement;
    }

    public boolean isEmpty() throws KeeperException, InterruptedException {
        return _zkClient.getChildren(_root, false).size() == 0;
    }
}
