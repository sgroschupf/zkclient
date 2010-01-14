/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import java.io.Serializable;
import java.util.List;

import org.I0Itec.zkclient.exception.ZkNoNodeException;

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
            _zkClient.createPersistentSequential(_root + "/" + ELEMENT_NAME + "-", element);
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
            } catch (ZkNoNodeException e) {
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

    public boolean isEmpty() {
        return _zkClient.getChildren(_root).size() == 0;
    }

    @SuppressWarnings("unchecked")
    private Element<T> getFirstElement() {
        try {
            while (true) {
                List<String> list = _zkClient.getChildren(_root);
                if (list.size() == 0) {
                    return null;
                }
                String elementName = getSmallestElement(list);

                try {
                    return new Element<T>(_root + "/" + elementName, (T) _zkClient.readData(_root + "/" + elementName));
                } catch (ZkNoNodeException e) {
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
