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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class ZkConnection implements IZkConnection {

    private static final Logger LOG = Logger.getLogger(ZkConnection.class);

    /** It is recommended to use quite large sessions timeouts for ZooKeeper. */
    private static final int DEFAULT_SESSION_TIMEOUT = 30000;

    private ZooKeeper _zk = null;
    private Lock _zookeeperLock = new ReentrantLock();

    private final String _servers;
    private final int _sessionTimeOut;

    public ZkConnection(String zkServers) {
        this(zkServers, DEFAULT_SESSION_TIMEOUT);
    }

    public ZkConnection(String zkServers, int sessionTimeOut) {
        _servers = zkServers;
        _sessionTimeOut = sessionTimeOut;
    }

    @Override
    public void connect(Watcher watcher) {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                throw new IllegalStateException("zk client has already been started");
            }
            try {
                LOG.debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
                _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
            } catch (IOException e) {
                throw new ZkException("Unable to connect to " + _servers, e);
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public void close() throws InterruptedException {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                LOG.debug("Closing ZooKeeper connected to " + _servers);
                _zk.close();
                _zk = null;
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }

    public void delete(String path) throws InterruptedException, KeeperException {
        _zk.delete(path, -1);
    }

    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.exists(path, watch) != null;
    }

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return _zk.getChildren(path, watch);
    }

    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, stat);
    }

    public Stat writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        return writeData(path, data, -1);
    }

    public Stat writeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return _zk.setData(path, data, version);
    }

    public States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }

    public ZooKeeper getZookeeper() {
        return _zk;
    }

    @Override
    public long getCreateTime(String path) throws KeeperException, InterruptedException {
        Stat stat = _zk.exists(path, false);
        if (stat != null) {
            return stat.getCtime();
        }
        return -1;
    }

    @Override
    public String getServers() {
        return _servers;
    }
}
