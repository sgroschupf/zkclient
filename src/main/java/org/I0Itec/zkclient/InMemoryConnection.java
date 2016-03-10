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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.util.ZkPathUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;

/**
 * Emulating a ZooKeeper server with few hash tables. Basically a mock class used for testing. Please avoid using this
 * as your ZK in production :)
 * 
 * Note that the addAuth is even more mocked than usual Since we have no authentication provider (i.e. Kerberos) around
 * we simply take the auth byte[] and convert it to string to get the Id scheme remains the same
 */
public class InMemoryConnection implements IZkConnection {

    public static class DataAndVersion {
        private byte[] _data;
        private int _version;
        private List<ACL> _acl;

        public DataAndVersion(byte[] data, int version, List<ACL> acl) {
            _data = data;
            _version = version;
            _acl = acl;
        }

        public DataAndVersion(byte[] data, int version) {
            this(data, version, null);
        }

        public byte[] getData() {
            return _data;
        }

        public int getVersion() {
            return _version;
        }

        public List<ACL> getAcl() {
            return _acl;
        }
    }

    private Lock _lock = new ReentrantLock(true);
    private Map<String, DataAndVersion> _data = new HashMap<String, DataAndVersion>();
    private Map<String, Long> _creationTime = new HashMap<String, Long>();
    private List<Id> _ids = new ArrayList<Id>();
    private final AtomicInteger sequence = new AtomicInteger(0);

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
    public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException {
        _lock.lock();
        try {

            if (mode.isSequential()) {
                final int newSequence = sequence.getAndIncrement();
                path = path + ZkPathUtil.leadingZeros(newSequence, 10);
            }

            if (exists(path, false)) {
                throw new KeeperException.NodeExistsException();
            }
            String parentPath = getParentPath(path);
            checkACL(parentPath, ZooDefs.Perms.CREATE);

            _data.put(path, new DataAndVersion(data, 0, acl));
            _creationTime.put(path, System.currentTimeMillis());
            checkWatch(_nodeWatches, path, EventType.NodeCreated);
            // we also need to send a child change event for the parent
            if (parentPath != null) {
                checkWatch(_nodeWatches, parentPath, EventType.NodeChildrenChanged);
            }
            return path;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return create(path, data, null, mode);
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
        this.delete(path, -1);
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        _lock.lock();
        try {
            if (!exists(path, false)) {
                throw new KeeperException.NoNodeException();
            }
            String parentPath = getParentPath(path);
            checkACL(parentPath, ZooDefs.Perms.DELETE);
            // If version isn't -1, check that it mateches
            if (version != -1) {
                DataAndVersion item = _data.get(path);
                if (item._version != version) {
                    throw KeeperException.create(Code.BADVERSION);
                }
            }
            _data.remove(path);
            _creationTime.remove(path);
            checkWatch(_nodeWatches, path, EventType.NodeDeleted);
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

        checkACL(path, ZooDefs.Perms.READ);
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
    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        if (watch) {
            installWatch(_dataWatches, path);
        }
        _lock.lock();
        try {
            DataAndVersion dataAndVersion = _data.get(path);
            if (dataAndVersion == null) {
                throw new ZkNoNodeException(new KeeperException.NoNodeException());
            }
            checkACL(path, ZooDefs.Perms.READ);
            byte[] bs = dataAndVersion.getData();
            if (stat != null)
                stat.setVersion(dataAndVersion.getVersion());
            return bs;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
        writeDataReturnStat(path, data, expectedVersion);
    }

    @Override
    public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
        int newVersion = -1;
        _lock.lock();
        try {
            checkWatch(_dataWatches, path, EventType.NodeDataChanged);
            if (!exists(path, false)) {
                throw new KeeperException.NoNodeException();
            }
            checkACL(path, ZooDefs.Perms.WRITE);
            newVersion = _data.get(path).getVersion() + 1;
            _data.put(path, new DataAndVersion(data, newVersion));
            String parentPath = getParentPath(path);
            if (parentPath != null) {
                checkWatch(_nodeWatches, parentPath, EventType.NodeChildrenChanged);
            }
        } finally {
            _lock.unlock();
        }
        Stat stat = new Stat();
        stat.setVersion(newVersion);
        return stat;
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

    public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
        List<OpResult> opResults = new ArrayList<OpResult>();
        for (Op op : ops) {
            if (Op.Check.class.isAssignableFrom(op.getClass())) {
                CheckVersionRequest check = (CheckVersionRequest) op.toRequestRecord();
                exists(check.getPath(), false);
                opResults.add(new OpResult.CheckResult());
            } else if (Op.Create.class.isAssignableFrom(op.getClass())) {
                CreateRequest create = (CreateRequest) op.toRequestRecord();
                String path = create(create.getPath(), create.getData(), CreateMode.fromFlag(create.getFlags()));
                opResults.add(new OpResult.CreateResult(path));
            } else if (Op.Delete.class.isAssignableFrom(op.getClass())) {
                DeleteRequest delete = (DeleteRequest) op.toRequestRecord();
                delete(delete.getPath());
                opResults.add(new OpResult.DeleteResult());
            } else if (Op.SetData.class.isAssignableFrom(op.getClass())) {
                SetDataRequest setData = (SetDataRequest) op.toRequestRecord();
                writeData(setData.getPath(), setData.getData(), setData.getVersion());
                opResults.add(new OpResult.SetDataResult(null));
            }
        }
        return opResults;
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        _ids.add(new Id(scheme, new String(auth)));
    }

    @Override
    public void setAcl(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        if (!exists(path, false)) {
            throw new KeeperException.NoNodeException();
        }

        DataAndVersion dataAndVersion = _data.get(path);
        if (version != dataAndVersion._version) {
            throw new KeeperException.BadVersionException();
        }

        checkACL(path, ZooDefs.Perms.ADMIN);

        _lock.lock();
        try {
            _data.put(path, new DataAndVersion(dataAndVersion.getData(), dataAndVersion.getVersion() + 1, acl));
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public Map.Entry<List<ACL>, Stat> getAcl(String path) throws KeeperException, InterruptedException {
        if (!exists(path, false)) {
            throw new KeeperException.NoNodeException();
        }

        DataAndVersion dataAndVersion = _data.get(path);

        Stat stat = new Stat();
        stat.setVersion(dataAndVersion.getVersion());
        stat.setCtime(_creationTime.get(path));

        return new AbstractMap.SimpleEntry<List<ACL>, Stat>(dataAndVersion.getAcl(), stat);
    }

    /***
     * 
     * @param path
     *            - path of znode we are accessing
     * @param perm
     *            - Privileges required for the action
     * @throws KeeperException.NoAuthException
     */
    private void checkACL(String path, int perm) throws KeeperException.NoAuthException {
        DataAndVersion node = _data.get(path);
        if (node == null) {
            return;
        }
        List<ACL> acl = node.getAcl();
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : _ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                    return;
                }
                for (Id authId : _ids) {
                    if (authId.getScheme().equals(id.getScheme()) && authId.getId().equals(id.getId())) {
                        return;
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }
}
