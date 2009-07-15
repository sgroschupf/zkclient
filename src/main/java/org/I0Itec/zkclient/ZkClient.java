package org.I0Itec.zkclient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

/**
 * Abstracts the interaction with zookeeper and allows permanent (not just one
 * time) watches on nodes in ZooKeeper
 * 
 */
public class ZkClient implements Watcher {

    private final static Logger LOG = Logger.getLogger(ZkClient.class);

    private static final int DEFAULT_TIMEOUT = 5000;

    private ZooKeeper _zk = null;
    private final ZkLock _zkEventLock = new ZkLock();

    private final Map<String, Set<IZkChildListener>> _childListener = new ConcurrentHashMap<String, Set<IZkChildListener>>();
    private final ConcurrentHashMap<String, Set<IZkDataListener<? extends Serializable>>> _dataListener = new ConcurrentHashMap<String, Set<IZkDataListener<? extends Serializable>>>();
    private final Set<IZkStateListener> _stateListener = new CopyOnWriteArraySet<IZkStateListener>();

    private final String _servers;
    private final int _sessionTimeOut;
    private boolean _shutdownTriggered;

    private KeeperState _currentState;

    public ZkClient(String zkServers, int connectionTimeout) throws IOException {
        _servers = zkServers;
        _sessionTimeOut = DEFAULT_TIMEOUT;
        connect(connectionTimeout);
    }

    public ZkClient(String zkServer) throws IOException {
        this(zkServer, Integer.MAX_VALUE);
    }

    /**
     * Starts a zookeeper client and waits until connected with one of the
     * zookeeper servers.
     * 
     * @param maxMsToWaitUntilConnected
     *            the milliseconds a method call will wait until the zookeeper
     *            client is connected with the server
     * @throws IOException
     */
    private void connect(final long maxMsToWaitUntilConnected) throws IOException {
        if (_zk != null) {
            throw new IllegalStateException("zk client has already been started");
        }
        boolean started = false;
        try {
            getEventLock().lockInterruptibly();
            _shutdownTriggered = false;
            _zk = new ZooKeeper(_servers, _sessionTimeOut, this);

            LOG.debug("Awaiting connection to Zookeeper server");
            if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS)) {
                throw new IOException("Unable to connect to zookeeper server within timeout: " + maxMsToWaitUntilConnected);
            }
            started = true;
        } catch (InterruptedException e) {
            States state = getZookeeperState();
            _zk = null;
            throw new IllegalStateException("Not connected with zookeeper server yet. Current state is " + state);
        } finally {
            getEventLock().unlock();

            // we should close the zookeeper instance, otherwise it would keep
            // on trying to connect
            if (!started && _zk != null) {
                try {
                    _zk.close();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Closes down the connection to zookeeper. This will remove all ephemeral
     * nodes within zookeeper this client created.
     * 
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        getEventLock().lock();
        try {
            _shutdownTriggered = true;
            if (_zk != null) {
                _zk.close();
                _zk = null;
            }
        } finally {
            getEventLock().unlock();
        }
    }

    /**
     * Returns a mutex all zookeeper events are synchronized aginst. So in case
     * you need to do something without getting any zookeeper event interruption
     * synchronize against this mutex. Also all threads waiting on this mutex
     * object will be notified on an event.
     * 
     * @return the mutex.
     */
    ZkLock getEventLock() {
        return _zkEventLock;
    }

    /**
     * Subscribes an {@link IZkChildListener} for permanent notifications for
     * children changes (adds and removes) of given path.
     * 
     * @param path
     * @param listener
     * @return list of children nodes for given path.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public List<String> subscribeChildChanges(final String path, final IZkChildListener listener) throws KeeperException, InterruptedException {
        addChildListener(path, listener);
        boolean success = false;
        try {
            // List<String> children = _zk.getChildren(path, true);
            List<String> children = getChildren(path);
            success = true;
            return children;
        } finally {
            if (!success) {
                unsubscribeChildChanges(path, listener);
            }
        }
    }

    private void addChildListener(final String path, final IZkChildListener listener) {
        Set<IZkChildListener> listeners;
        synchronized (_childListener) {
            listeners = _childListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<IZkChildListener>();
                _childListener.put(path, listeners);
            }
        }
        listeners.add(listener);
    }

    public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
        synchronized (_childListener) {
            final Set<IZkChildListener> listeners = _childListener.get(path);
            if (listeners != null) {
                listeners.remove(childListener);
            }
        }
    }

    /**
     * Subscribes notifications for permanent notifications for data changes on
     * the given node path.
     * 
     * @param path
     * @param listener
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void subscribeDataChanges(final String path, final IZkDataListener<? extends Serializable> listener) throws KeeperException, InterruptedException {
        addDataListener(path, listener);
        boolean success = false;
        try {
            // TODO sg: Review this: This blocks if there is a connection loss
            retryUntilConnected(new Callable<Object>() {

                @Override
                public Object call() throws Exception {
                    _zk.exists(path, true);
                    return null;
                }
            });
            success = true;
        } finally {
            if (!success) {
                unsubscribeDataChanges(path, listener);
            }
        }
    }

    private void addDataListener(final String path, final IZkDataListener<? extends Serializable> listener) {
        Set<IZkDataListener<? extends Serializable>> listeners;
        synchronized (_dataListener) {
            listeners = _dataListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<IZkDataListener<? extends Serializable>>();
                _dataListener.put(path, listeners);
            }
        }
        listeners.add(listener);
    }

    public void unsubscribeDataChanges(String path, IZkDataListener<?> dataListener) {
        synchronized (_dataListener) {
            final Set<IZkDataListener<? extends Serializable>> listeners = _dataListener.get(path);
            if (listeners != null) {
                listeners.remove(dataListener);
            }
        }
    }

    public void unsubscribeStateChanges(IZkStateListener stateListener) {
        synchronized (_stateListener) {
            _stateListener.remove(stateListener);
        }
    }

    public void subscribeStateChanges(final IZkStateListener listener) {
        synchronized (_stateListener) {
            _stateListener.add(listener);
        }
    }

    /**
     * Unsubscribe all listeners from zk events.
     */
    public void unsubscribeAll() {
        synchronized (_childListener) {
            _childListener.clear();
        }
        synchronized (_dataListener) {
            _dataListener.clear();
        }
        synchronized (_stateListener) {
            _stateListener.clear();
        }
    }

    /**
     * Creates an node for given path without any data.
     * 
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    public void createPersistent(final String path) throws KeeperException, InterruptedException, IOException {
        create(path, null, CreateMode.PERSISTENT);
    }

    /**
     * Creates an node for given path with given {@link Writable} data.
     * 
     * @param path
     * @param serializable
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    public void createPersistent(final String path, final Serializable serializable) throws KeeperException, InterruptedException, IOException {
        create(path, serializable, CreateMode.PERSISTENT);
    }

    /**
     * Creates an ephemeral node for give path. In case the client that created
     * that node disconnects the node is removed.
     * 
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    public void createEphemeral(final String path) throws KeeperException, InterruptedException, IOException {
        create(path, null, CreateMode.EPHEMERAL);
    }

    /**
     * Creates and ephemeral node with given data of the writable.
     * 
     * @param path
     * @param serializable
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    public void createEphemeral(final String path, final Serializable serializable) throws KeeperException, InterruptedException, IOException {
        create(path, serializable, CreateMode.EPHEMERAL);
    }

    public String createEphemeralSequential(final String path, final Serializable serializable) throws KeeperException, InterruptedException, IOException {
        return create(path, serializable, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public String create(final String path, final Serializable serializable, final CreateMode mode) throws KeeperException, InterruptedException, IOException {
        assert path != null;
        final byte[] data = serializable == null ? null : writableToByteArray(serializable);

        // TODO sg: Review this: This blocks if there is a connection loss
        return retryUntilConnected(new Callable<String>() {

            @Override
            public String call() throws Exception {
                return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
            }
        });
        // return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }

    private byte[] writableToByteArray(final Serializable serializable) throws IOException {
        ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
        stream.writeObject(serializable);
        stream.close();
        return byteArrayOS.toByteArray();
    }

    /**
     * Deletes a given path. For recursive deletes use
     * {@link #deleteRecursive(String)}.
     * 
     * @param path
     * @return true when the node was deleted, false if it didn't exist
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean delete(final String path) throws InterruptedException, KeeperException {
        try {
            // TODO sg: review this: This blocks until there is a connection to
            // zookeeper
            retryUntilConnected(new Callable<Object>() {

                @Override
                public Object call() throws Exception {
                    _zk.delete(path, -1);
                    return null;
                }
            });

            // _zk.delete(path, -1);
            return true;
        } catch (NoNodeException e) {
            return false;
        }
    }

    public <T> T retryUntilConnected(Callable<T> callable) throws KeeperException, InterruptedException {
        while (true) {
            try {
                return callable.call();
            } catch (ConnectionLossException e) {
                // we give the event thread some time to update the status to
                // disconnected
                Thread.yield();
                waitUntilConnected();
            } catch (KeeperException e) {
                throw e;
            } catch (Exception e) {
                throw ExceptionUtil.convertToRuntimeException(e);
            }
        }
    }

    public void waitUntilConnected() throws InterruptedException {
        waitUntilConnected(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public boolean waitUntilConnected(long time, TimeUnit timeUnit) throws InterruptedException {
        return waitForKeeperState(KeeperState.SyncConnected, time, timeUnit);
    }

    public boolean waitForKeeperState(KeeperState keeperState, long time, TimeUnit timeUnit) throws InterruptedException {
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

        LOG.info("Waiting for keeper state " + keeperState);
        getEventLock().lockInterruptibly();
        try {
            boolean stillWaiting = true;
            while (_currentState != keeperState) {
                if (!stillWaiting) {
                    return false;
                }
                stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeout);
            }
            LOG.info("State is " + _currentState);
            return true;
        } finally {
            getEventLock().unlock();
        }
    }

    /**
     * Deletes a path and all children recursivly.
     * 
     * @param path
     * @return whether the directory was removed.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public boolean deleteRecursive(final String path) throws InterruptedException, KeeperException {
        List<String> children;
        try {
            children = _zk.getChildren(path, false);
        } catch (NoNodeException e) {
            return true;
        }
        
        for (String subPath : children) {
            if (!deleteRecursive(path + "/" + subPath)) {
                return false;
            }
        }

        return delete(path);
    }

    /**
     * Checks if a node with given path exists.
     * 
     * @param path
     * @return whether the node exists.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public boolean exists(final String path) throws KeeperException, InterruptedException {
        return exists(path, false);
    }

    public boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
        // TODO sg: review this: This blocks until there is a connection to
        // zookeeper
        return retryUntilConnected(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return _zk.exists(path, watch) != null;
            }
        });
    }

    /**
     * Returns an list of all children names of given path.
     * 
     * @param path
     * @return the children.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        Set<IZkChildListener> listeners = _childListener.get(path);
        boolean watch = listeners != null && listeners.size() > 0;
        
        return getChildren(path, watch);
    }

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
        // TODO sg: Review this: This blocks if there is a connection loss
        return retryUntilConnected(new Callable<List<String>>() {

            @Override
            public List<String> call() throws Exception {
                return _zk.getChildren(path, watch);
            }
        });
    }

    public int countChildren(String path) throws KeeperException, InterruptedException {
        int childCount = 0;
        if (exists(path)) {
            childCount = getChildren(path).size();
        }
        return childCount;
    }

    @Override
    public void process(WatchedEvent event) {
        boolean stateChanged = event.getPath() == null;
        boolean znodeChanged = event.getPath() != null;
        boolean dataChanged = event.getType() == Watcher.Event.EventType.NodeDataChanged || event.getType() == Watcher.Event.EventType.NodeChildrenChanged
                || event.getType() == Watcher.Event.EventType.NodeCreated;

        getEventLock().lock();
        try {
            if (_shutdownTriggered) {
                LOG.debug("ignoring event '{" + event.getType() + " | " + event.getPath() + "}' since shutdown triggered");
                return;
            }
            if (stateChanged) {
                processStateChanged(event);
            }
            if (dataChanged) {
                processDataOrChildChange(event);
            }
        } finally {
            if (stateChanged) {
                getEventLock().getStateChangedCondition().signalAll();
            }
            if (znodeChanged) {
                getEventLock().getZNodeEventCondition().signalAll();
            }
            if (dataChanged) {
                getEventLock().getDataChangedCondition().signalAll();
            }
            getEventLock().unlock();
        }
    }

    private void processStateChanged(WatchedEvent event) {
        LOG.warn("zookeeper state changed (" + event.getState() + ")");
        setCurrentState(event.getState());
        if (_shutdownTriggered) {
            return;
        }
        try {
            for (IZkStateListener stateListener : _stateListener) {
                stateListener.handleStateChanged(event.getState());
            }

            if (event.getState() == KeeperState.Expired) {
                close();
                connect(Integer.MAX_VALUE);
                for (IZkStateListener stateListener : _stateListener) {
                    try {
                        stateListener.handleNewSession();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    } catch (Throwable t) {
                        LOG.warn("Handler run into an error.", t);
                    }
                }

                if (event.getState() == KeeperState.SyncConnected) {
                    // re-register all subscriptions
                    synchronized (_childListener) {
                        for (Entry<String, Set<IZkChildListener>> entry : _childListener.entrySet()) {
                            resubscribeChildPath(entry.getKey(), entry.getValue());
                        }
                    }
                    synchronized (_dataListener) {
                        for (Entry<String, Set<IZkDataListener<? extends Serializable>>> entry : _dataListener.entrySet()) {
                            resubscribeDataPath(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Exception while restarting zk client", e);
        }
    }

    private void setCurrentState(KeeperState currentState) {
        getEventLock().lock();
        try {
            _currentState = currentState;
        } finally {
            getEventLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void processDataOrChildChange(WatchedEvent event) {
        // ZkEventType eventType = ZkEventType.getMappedType(event.getType());
        final String path = event.getPath();

        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            Set<IZkChildListener> childListeners = _childListener.get(path);
            if (childListeners != null && !childListeners.isEmpty()) {

                List<String> children = resubscribeChildPath(path, childListeners);

                for (final IZkChildListener listener : childListeners) {
                    try {
                        listener.handleChildChange(event.getPath(), children);
                    } catch (final Throwable e) {
                        LOG.error("Faild to process event with listener: " + listener, e);
                    }
                }
            }
        } else {
            Set<IZkDataListener<? extends Serializable>> listeners = _dataListener.get(path);
            if (listeners != null && !listeners.isEmpty()) {
                byte[] data = resubscribeDataPath(path, listeners);

                for (final IZkDataListener listener : listeners) {
                    try {
                        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                            listener.handleDataAdded(event.getPath(), readSerializable(data));
                        } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                            listener.handleDataChange(event.getPath(), readSerializable(data));
                        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                            listener.handleDataDeleted(event.getPath());
                        } else {
                            LOG.error("Received a unknown event, ignoring: " + event.getType());
                        }
                    } catch (final Throwable e) {
                        LOG.error("Faild to process event " + event.getType() + " with listener: " + listener, e);
                    }
                }
            }
        }
    }

    private byte[] resubscribeDataPath(String path, Set<IZkDataListener<? extends Serializable>> listeners) {
        byte[] data = null;
        try {
            data = _zk.getData(path, true, null);
        } catch (final Exception e) {
            for (final IZkDataListener<?> listener : listeners) {
                unsubscribeDataChanges(path, listener);
            }
            LOG.fatal("resubscription for data changes on path '" + path + "' failed. removing listeners", e);
        }
        return data;
    }

    private List<String> resubscribeChildPath(String path, final Set<IZkChildListener> childListeners) {
        List<String> children;
        try {
            children = _zk.getChildren(path, true);
        } catch (final Exception e) {
            LOG.fatal("re-subscription for child changes on path '" + path + "' failed. removing listeners", e);
            children = Collections.emptyList();
            childListeners.clear();
        }
        return children;
    }

    /**
     * Reads that data of given path into a writeable instance. Make sure you
     * use the same writable implementation as you used to write the data.
     * 
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> T readData(final String path) throws KeeperException, InterruptedException, IOException {
        final Set<IZkDataListener<? extends Serializable>> set = _dataListener.get(path);
        final boolean watch = set != null && set.size() > 0;
        // TODO sg: Review this: This blocks if there is a connection loss
        byte[] data = retryUntilConnected(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return _zk.getData(path, watch, null);
            }
        });
        return (T) readSerializable(data);
    }

    @SuppressWarnings("unchecked")
    private <T extends Serializable> T readSerializable(byte[] data) throws IOException {
        if (data == null) {
            return null;
        }
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(data));
            Object object = inputStream.readObject();
            return (T) object;
        } catch (ClassNotFoundException e) {
            throw new IOException("Unable to find object class", e);
        }
    }

    /**
     * Shows the full node structure of the Zookeeper server. Useful for
     * debugging.
     * 
     * @param output
     *            The output stream to write the debugging information to.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void showFolders(OutputStream output) throws KeeperException, InterruptedException {
        final int level = 1;
        final StringBuilder builder = new StringBuilder();
        final String startPath = "/";
        addChildren(level, builder, startPath);
        try {
            output.write(builder.toString().getBytes());
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void addChildren(final int level, final StringBuilder builder, final String startPath) throws KeeperException, InterruptedException {
        final List<String> children = getChildren(startPath);
        for (final String node : children) {
            builder.append(getSpaces(level - 1) + "'-" + "+" + node + "\n");

            String nestedPath;
            if (startPath.endsWith("/")) {
                nestedPath = startPath + node;
            } else {
                nestedPath = startPath + "/" + node;
            }

            addChildren(level + 1, builder, nestedPath);
        }
    }

    private String getSpaces(final int level) {
        String s = "";
        for (int i = 0; i < level; i++) {
            s += "  ";
        }
        return s;
    }

    /**
     * Creates a node and writes data of writable into the given path.
     * 
     * @param path
     * @param serializable
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    public void writeData(final String path, final Serializable serializable) throws KeeperException, InterruptedException, IOException {
        final byte[] data = writableToByteArray(serializable);
        // TODO sg: Review this: This blocks if there is a connection loss
        retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                _zk.setData(path, data, -1);
                return null;
            }
        });
    }

    private States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }

    public static class ZkLock extends ReentrantLock {

        private static final long serialVersionUID = 1L;

        private Condition _dataChangedCondition = newCondition();
        private Condition _stateChangedCondition = newCondition();
        private Condition _zNodeEventCondition = newCondition();

        /**
         * This condition will be signaled if a zookeeper event was processed
         * and the event contains a data/child change.
         * 
         * @return the condition.
         */
        public Condition getDataChangedCondition() {
            return _dataChangedCondition;
        }

        /**
         * This condition will be signaled if a zookeeper event was processed
         * and the event contains a state change (connected, disconnected,
         * session expired, etc ...).
         * 
         * @return the condition.
         */
        public Condition getStateChangedCondition() {
            return _stateChangedCondition;
        }

        /**
         * This condition will be signaled if any znode related zookeeper event
         * was received.
         * 
         * @return the condition.
         */
        public Condition getZNodeEventCondition() {
            return _zNodeEventCondition;
        }
    }

    ZooKeeper getZookeeper() {
        return _zk;
    }

    public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) throws InterruptedException, KeeperException {
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

        if (exists(path)) {
            return true;
        }

        LOG.info("Waiting until znode '" + path + "' becomes available.");
        getEventLock().lockInterruptibly();
        try {
            boolean stillWaiting = true;
            while (!exists(path, true)) {
                if (!stillWaiting) {
                    return false;
                }
                stillWaiting = getEventLock().getZNodeEventCondition().awaitUntil(timeout);
            }
            return true;
        } finally {
            getEventLock().unlock();
        }
    }
}
