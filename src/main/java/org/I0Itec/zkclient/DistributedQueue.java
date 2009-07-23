package org.I0Itec.zkclient;

import java.io.Serializable;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

public class DistributedQueue<T extends Serializable> {

    private static class Element<T> {
        private String _name;
        private T _data;

        public Element(String name, T data) {
            _name = name;
            _data = data;
        }

        public String getName() {
            return _name;
        }

        public T getData() {
            return _data;
        }
    }

    private ZkClient _zkClient;
    private String _root;

    private static final String ELEMENT_NAME = "element";

    public DistributedQueue(ZkClient zkClient, String root) {
        _zkClient = zkClient;
        _root = root;
    }

    public boolean offer(T element) {
        try {
            _zkClient.create(_root + "/" + ELEMENT_NAME + "-", element, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
        return true;
    }

    public T poll() {
        while (true) {
            Element<T> element = getFirstElement();
            if (element == null) {
                return null;
            }

            try {
                _zkClient.delete(element.getName());
                return element.getData();
            } catch (NoNodeException e) {
                // somebody else picked up the element first, so we have to
                // retry with the new first element
            } catch (Exception e) {
                throw ExceptionUtil.convertToRuntimeException(e);
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

    @SuppressWarnings("unchecked")
    private Element<T> getFirstElement() {
        try {
            while (true) {
                List<String> list = _zkClient.getChildren(_root, true);
                if (list.size() == 0) {
                    return null;
                }
                String elementName = getSmallestElement(list);

                try {
                    return new Element<T>(_root + "/" + elementName, (T) _zkClient.readData(_root + "/" + elementName));
                } catch (NoNodeException e) {
                    // somebody else picked up the element first, so we have to
                    // retry with the new first element
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
    }

    public T peek() {
        Element<T> element = getFirstElement();
        if (element == null) {
            return null;
        }
        return element.getData();
    }
}
