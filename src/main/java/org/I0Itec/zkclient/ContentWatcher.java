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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <T>
 *            The data type that is being watched.
 */
public final class ContentWatcher<T extends Object> implements IZkDataListener {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWatcher.class);

    private Lock _contentLock = new ReentrantLock(true);
    private Condition _contentAvailable = _contentLock.newCondition();

    private Holder<T> _content;
    private String _fileName;
    private ZkClient _zkClient;

    public ContentWatcher(ZkClient zkClient, String fileName) {
        _fileName = fileName;
        _zkClient = zkClient;
    }

    public void start() {
        _zkClient.subscribeDataChanges(_fileName, this);
        readData();
        LOG.debug("Started ContentWatcher");
    }

    @SuppressWarnings("unchecked")
    private void readData() {
        try {
            setContent((T) _zkClient.readData(_fileName));
        } catch (ZkNoNodeException e) {
            // ignore if the node has not yet been created
        }
    }

    public void stop() {
        _zkClient.unsubscribeDataChanges(_fileName, this);
    }

    public void setContent(T data) {
        LOG.debug("Received new data: " + data);
        _contentLock.lock();
        try {
            _content = new Holder<T>(data);
            _contentAvailable.signalAll();
        } finally {
            _contentLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleDataChange(String dataPath, Object data) {
        setContent((T) data);
    }

    @Override
    public void handleDataDeleted(String dataPath) {
        // ignore
    }

    public T getContent() throws InterruptedException {
        _contentLock.lock();
        try {
            while (_content == null) {
                _contentAvailable.await();
            }
            return _content.get();
        } finally {
            _contentLock.unlock();
        }
    }
}
