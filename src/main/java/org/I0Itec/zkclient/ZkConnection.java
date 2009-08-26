package org.I0Itec.zkclient;

import java.io.IOException;
import java.util.List;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class ZkConnection implements IZkConnection {

    /** It is recommended to use quite large sessions timeouts for Zookeeper. */
    private static final int DEFAULT_SESSION_TIMEOUT = 30000;
    private ZooKeeper _zk = null;

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
        if (_zk != null) {
            throw new IllegalStateException("zk client has already been started");
        }
        try {
            _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
        } catch (IOException e) {
            throw new ZkException("Unable to connect to " + _servers, e);
        }
    }
    
    public void close() throws InterruptedException {
        if (_zk != null) {
            _zk.close();
            _zk = null;
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

    public byte[] readData(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, null);
    }

    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        _zk.setData(path, data, -1);
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
        } else {
          return -1;
        }
    }
}
