package org.I0Itec.zkclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

public class InMemoryConnection implements IZkConnection {

    private Lock _lock = new ReentrantLock(true);
    private Map<String, byte[]> _data = new HashMap<String, byte[]>();
    private Map<String, Long> _creationTime = new HashMap<String, Long>();

    private Set<String> _dataWatches = new HashSet<String>();
    private Set<String> _nodeWatches = new HashSet<String>();
    private EventThread _eventThread;

    private class EventThread extends Thread {

        private Watcher _watcher;
        private BlockingQueue<WatchedEvent> _blockingQueue = new LinkedBlockingDeque<WatchedEvent>();

        public EventThread(Watcher watcher) {
            _watcher = watcher;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    _watcher.process(_blockingQueue.take());
                }
            } catch (InterruptedException e) {
                // stop event thread
            }
        }

        public void send(WatchedEvent event) {
            _blockingQueue.add(event);
        }
    }

    public InMemoryConnection() {
        try {
            create("/", null, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw ZkException.create(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZkInterruptedException(e);
        }
    }

    @Override
    public void close() throws InterruptedException {
        _lock.lockInterruptibly();
        try {
            if (_eventThread != null) {
                _eventThread.interrupt();
                _eventThread.join();
                _eventThread = null;
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void connect(Watcher watcher) {
        _lock.lock();
        try {
            if (_eventThread != null) {
                throw new IllegalStateException("Already connected.");
            }
            _eventThread = new EventThread(watcher);
            _eventThread.start();
            _eventThread.send(new WatchedEvent(null, KeeperState.SyncConnected, null));
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        _lock.lock();
        try {
            if (exists(path, false)) {
                throw new KeeperException.NodeExistsException();
            }

            _data.put(path, data);
            _creationTime.put(path, System.currentTimeMillis());
            checkWatch(_nodeWatches, path, EventType.NodeCreated);
            // we also need to send a child change event for the parent
            String parentPath = getParentPath(path);
            if (parentPath != null) {
                checkWatch(_nodeWatches, parentPath, EventType.NodeChildrenChanged);
            }
            return path;
        } finally {
            _lock.unlock();
        }
    }

    private String getParentPath(String path) {
        int lastIndexOf = path.lastIndexOf("/");
        if (lastIndexOf == -1 || lastIndexOf == 0) {
            return null;
        }
        return path.substring(0, lastIndexOf);
    }

    @Override
    public void delete(String path) throws InterruptedException, KeeperException {
        _lock.lock();
        try {
            if (!exists(path, false)) {
                throw new KeeperException.NoNodeException();
            }
            _data.remove(path);
            _creationTime.remove(path);
            checkWatch(_nodeWatches, path, EventType.NodeDeleted);
            String parentPath = getParentPath(path);
            if (parentPath != null) {
                checkWatch(_nodeWatches, parentPath, EventType.NodeChildrenChanged);
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        _lock.lock();
        try {
            if (watch) {
                installWatch(_nodeWatches, path);
            }
            return _data.containsKey(path);
        } finally {
            _lock.unlock();
        }
    }

    private void installWatch(Set<String> watches, String path) {
        watches.add(path);
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        if (!exists(path, false)) {
            throw KeeperException.create(Code.NONODE, path);
        }
        if (exists(path, false) && watch) {
            installWatch(_nodeWatches, path);
        }

        ArrayList<String> children = new ArrayList<String>();
        String[] directoryStack = path.split("/");
        Set<String> keySet = _data.keySet();

        for (String string : keySet) {
            if (string.startsWith(path)) {
                String[] stack = string.split("/");
                // is one folder level below the one we loockig for and starts
                // with path...
                if (stack.length == directoryStack.length + 1) {
                    children.add(stack[stack.length - 1]);
                }
            }

        }
        return children;
    }

    @Override
    public States getZookeeperState() {
        _lock.lock();
        try {
            if (_eventThread == null) {
                return States.CLOSED;
            }
            return States.CONNECTED;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public byte[] readData(String path, boolean watch) throws KeeperException, InterruptedException {
        if (watch) {
            installWatch(_dataWatches, path);
        }
        _lock.lock();
        try {
            byte[] bs = _data.get(path);
            if (bs == null) {
                throw new ZkNoNodeException(new KeeperException.NoNodeException());
            }
            return bs;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        _lock.lock();
        try {
            checkWatch(_dataWatches, path, EventType.NodeDataChanged);
            if (!exists(path, false)) {
                throw new KeeperException.NoNodeException();
            }
            _data.put(path, data);
            String parentPath = getParentPath(path);
            if (parentPath != null) {
                checkWatch(_nodeWatches, parentPath, EventType.NodeChildrenChanged);
            }
        } finally {
            _lock.unlock();
        }
    }

    private void checkWatch(Set<String> watches, String path, EventType eventType) {
        if (watches.contains(path)) {
            watches.remove(path);
            _eventThread.send(new WatchedEvent(eventType, KeeperState.SyncConnected, path));
        }
    }

    @Override
    public long getCreateTime(String path) {
        Long time = _creationTime.get(path);
        if (time == null) {
            return -1;
        }
        return time;
    }

    @Override
    public String getServers() {
        return "mem";
    }
}
