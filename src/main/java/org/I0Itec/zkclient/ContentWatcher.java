package org.I0Itec.zkclient;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * @param <T>
 *            The data type that is being watched.
 */
public final class ContentWatcher<T extends Serializable> implements IZkDataListener<T>, IZkStateListener {

    private static final Logger LOG = Logger.getLogger(ContentWatcher.class);

    private Lock _contentLock = new ReentrantLock(true);
    private Condition _contentAvailable = _contentLock.newCondition();

    private Holder<T> _content;
    private String _fileName;
    private ZkClient _zkClient;

    public ContentWatcher(ZkClient zkClient, String fileName) {
        _fileName = fileName;
        _zkClient = zkClient;
    }

    public void start() throws KeeperException, InterruptedException, IOException {
        _zkClient.subscribeDataChanges(_fileName, this);
        _zkClient.subscribeStateChanges(this);
        readData();
    }

    @SuppressWarnings("unchecked")
    private void readData() throws KeeperException, InterruptedException, IOException {
        try {
            setContent((T) _zkClient.readData(_fileName));
        } catch (NoNodeException e) {
            // ignore if the node has not yet been created
        }
    }

    public void stop() {
        _zkClient.unsubscribeDataChanges(_fileName, this);
        _zkClient.unsubscribeStateChanges(this);
    }

    @Override
    public void handleDataAdded(String dataPath, T data) {
        setContent(data);
    }

    public void setContent(T data) {
        LOG.info("Received new data: " + data);
        _contentLock.lock();
        try {
            _content = new Holder<T>(data);
            _contentAvailable.signalAll();
        } finally {
            _contentLock.unlock();
        }
    }

    @Override
    public void handleDataChange(String dataPath, T data) {
        setContent(data);
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

    @Override
    public void handleNewSession() throws Exception {
        // nothing to do
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        if (state == KeeperState.SyncConnected) {
            readData();
        }
    }
}
