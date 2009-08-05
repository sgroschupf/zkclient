package org.I0Itec.zkclient;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

public class ZkConnection implements IZkConnection {

    private static final int DEFAULT_TIMEOUT = 5000;
    private ZooKeeper _zk = null;

    private final String _servers;
    private final int _sessionTimeOut;

    public ZkConnection(String zkServers) {
        _servers = zkServers;
        _sessionTimeOut = DEFAULT_TIMEOUT;
    }

    @Override
    public void connect(Watcher watcher) throws IOException {
        if (_zk != null) {
            throw new IllegalStateException("zk client has already been started");
        }
        _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
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

    public boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
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

}
