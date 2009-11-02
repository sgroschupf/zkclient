package org.I0Itec.zkclient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkEventThread.ZkEvent;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Abstracts the interaction with zookeeper and allows permanent (not just one time) watches on nodes in ZooKeeper
 */
public class ZkClient implements Watcher {

    private final static Logger LOG = Logger.getLogger(ZkClient.class);

    private IZkConnection _connection;
    private final Map<String, Set<IZkChildListener>> _childListener = new ConcurrentHashMap<String, Set<IZkChildListener>>();
    private final ConcurrentHashMap<String, Set<IZkDataListener>> _dataListener = new ConcurrentHashMap<String, Set<IZkDataListener>>();
    private final Set<IZkStateListener> _stateListener = new CopyOnWriteArraySet<IZkStateListener>();
    private KeeperState _currentState;
    private final ZkLock _zkEventLock = new ZkLock();
    private boolean _shutdownTriggered;
    private ZkEventThread _eventThread;
    // TODO PVo remove this later
    private Thread _zookeeperEventThread;

    public ZkClient(IZkConnection connection) {
        this(connection, Integer.MAX_VALUE);
    }

    public ZkClient(IZkConnection connection, int connectionTimeout) {
        _connection = connection;
        connect(connectionTimeout, this);
    }

    public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
        this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout);
    }

    public ZkClient(String zkServers, int connectionTimeout) {
        this(new ZkConnection(zkServers), connectionTimeout);
    }

    public ZkClient(String serverstring) {
        this(serverstring, Integer.MAX_VALUE);
    }

    public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
        synchronized (_childListener) {
            Set<IZkChildListener> listeners = _childListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<IZkChildListener>();
                _childListener.put(path, listeners);
            }
            listeners.add(listener);
        }
        return watchForChilds(path);
    }

    public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
        synchronized (_childListener) {
            final Set<IZkChildListener> listeners = _childListener.get(path);
            if (listeners != null) {
                listeners.remove(childListener);
            }
        }
    }

    public void subscribeDataChanges(String path, IZkDataListener listener) {
        Set<IZkDataListener> listeners;
        synchronized (_dataListener) {
            listeners = _dataListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<IZkDataListener>();
                _dataListener.put(path, listeners);
            }
            listeners.add(listener);
        }
        watchForData(path);
        LOG.debug("Subscribed data changes for " + path);
    }

    public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
        synchronized (_dataListener) {
            final Set<IZkDataListener> listeners = _dataListener.get(path);
            if (listeners != null) {
                listeners.remove(dataListener);
            }
        }
    }

    public void subscribeStateChanges(final IZkStateListener listener) {
        synchronized (_stateListener) {
            _stateListener.add(listener);
        }
    }

    public void unsubscribeStateChanges(IZkStateListener stateListener) {
        synchronized (_stateListener) {
            _stateListener.remove(stateListener);
        }
    }

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

    // </listeners>

    /**
     * Create a persistent node.
     * 
     * @param path
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public void createPersistent(String path) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        create(path, null, CreateMode.PERSISTENT);
    }

    /**
     * Create a persistent node.
     * 
     * @param path
     * @param serializable
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public void createPersistent(String path, Serializable serializable) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        create(path, serializable, CreateMode.PERSISTENT);
    }

    /**
     * Create a persistent, sequental node.
     * 
     * @param path
     * @param serializable
     * @return create node's path
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public String createPersistentSequential(String path, Serializable serializable) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        return create(path, serializable, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    /**
     * Create an ephemeral node.
     * 
     * @param path
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public void createEphemeral(final String path) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        create(path, null, CreateMode.EPHEMERAL);
    }

    /**
     * Create a node.
     * 
     * @param path
     * @param serializable
     * @param mode
     * @return create node's path
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public String create(final String path, Serializable serializable, final CreateMode mode) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        if (path == null) {
            throw new NullPointerException("path must not be null.");
        }
        final byte[] data = serializable == null ? null : toByteArray(serializable);

        return retryUntilConnected(new Callable<String>() {

            @Override
            public String call() throws Exception {
                return _connection.create(path, data, mode);
            }
        });
    }

    private byte[] toByteArray(Serializable serializable) {
        try {
            ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
            ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
            stream.writeObject(serializable);
            stream.close();
            return byteArrayOS.toByteArray();
        } catch (IOException e) {
            throw new ZkMarshallingError(e);
        }
    }

    /**
     * Create an ephemeral node.
     * 
     * @param path
     * @param serializable
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public void createEphemeral(final String path, final Serializable serializable) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        create(path, serializable, CreateMode.EPHEMERAL);
    }

    /**
     * Create an ephemeral, sequential node.
     * 
     * @param path
     * @param serializable
     * @return created path
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs
     */
    public String createEphemeralSequential(final String path, final Serializable serializable) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        return create(path, serializable, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public void process(WatchedEvent event) {
        LOG.debug("Received event: " + event);
        _zookeeperEventThread = Thread.currentThread();

        boolean stateChanged = event.getPath() == null;
        boolean znodeChanged = event.getPath() != null;
        boolean dataChanged = event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted || event.getType() == EventType.NodeCreated
                || event.getType() == EventType.NodeChildrenChanged;

        getEventLock().lock();
        try {

            // We might have to install child change event listener if a new node was created
            if (getShutdownTrigger()) {
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

                // If the session expired we have to signal all conditions, because watches might have been removed and
                // there is no guarantee that those
                // conditions will be signaled at all after an Expired event
                // TODO PVo write a test for this
                if (event.getState() == KeeperState.Expired) {
                    getEventLock().getZNodeEventCondition().signalAll();
                    getEventLock().getDataChangedCondition().signalAll();
                    // We also have to notify all listeners that something might have changed
                    fireAllEvents();
                }
            }
            if (znodeChanged) {
                getEventLock().getZNodeEventCondition().signalAll();
            }
            if (dataChanged) {
                getEventLock().getDataChangedCondition().signalAll();
            }
            getEventLock().unlock();
            LOG.debug("Leaving process event");
        }
    }

    private void fireAllEvents() {
        for (Entry<String, Set<IZkChildListener>> entry : _childListener.entrySet()) {
            fireChildChangedEvents(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Set<IZkDataListener>> entry : _dataListener.entrySet()) {
            fireDataChangedEvents(entry.getKey(), entry.getValue());
        }
    }

    public List<String> getChildren(String path) {
        return getChildren(path, hasListeners(path));
    }

    private List<String> getChildren(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return _connection.getChildren(path, watch);
            }
        });
    }

    /**
     * Counts number of children for the given path.
     * 
     * @param path
     * @return number of children or 0 if path does not exist.
     */
    public int countChildren(String path) {
        try {
            return getChildren(path).size();
        } catch (ZkNoNodeException e) {
            return 0;
        }
    }

    private boolean exists(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return _connection.exists(path, watch);
            }
        });
    }

    public boolean exists(final String path) {
        return exists(path, hasListeners(path));
    }

    private void processStateChanged(WatchedEvent event) {
        LOG.info("zookeeper state changed (" + event.getState() + ")");
        setCurrentState(event.getState());
        if (getShutdownTrigger()) {
            return;
        }
        try {
            fireStateChangedEvent(event.getState());

            if (event.getState() == KeeperState.Expired) {
                reconnect();
                fireNewSessionEvents();
            }
        } catch (final Exception e) {
            throw new RuntimeException("Exception while restarting zk client", e);
        }
    }

    private void fireNewSessionEvents() {
        for (final IZkStateListener stateListener : _stateListener) {
            _eventThread.send(new ZkEvent("New session event sent to " + stateListener.getClass().getName()) {

                @Override
                public void run() throws Exception {
                    stateListener.handleNewSession();
                }
            });
        }
    }

    private void fireStateChangedEvent(final KeeperState state) {
        for (final IZkStateListener stateListener : _stateListener) {
            _eventThread.send(new ZkEvent("State changed to " + state + " sent to " + stateListener.getClass().getName()) {

                @Override
                public void run() throws Exception {
                    stateListener.handleStateChanged(state);
                }
            });
        }
    }

    private boolean hasListeners(String path) {
        Set<IZkDataListener> dataListeners = _dataListener.get(path);
        if (dataListeners != null && dataListeners.size() > 0) {
            return true;
        }
        Set<IZkChildListener> childListeners = _childListener.get(path);
        if (childListeners != null && childListeners.size() > 0) {
            return true;
        }
        return false;
    }

    public boolean deleteRecursive(String path) {
        List<String> children;
        try {
            children = getChildren(path, false);
        } catch (ZkNoNodeException e) {
            return true;
        }

        for (String subPath : children) {
            if (!deleteRecursive(path + "/" + subPath)) {
                return false;
            }
        }

        return delete(path);
    }

    private void processDataOrChildChange(WatchedEvent event) {
        // ZkEventType eventType = ZkEventType.getMappedType(event.getType());
        final String path = event.getPath();

        if (event.getType() == EventType.NodeChildrenChanged || event.getType() == EventType.NodeCreated || event.getType() == EventType.NodeDeleted) {
            Set<IZkChildListener> childListeners = _childListener.get(path);
            if (childListeners != null && !childListeners.isEmpty()) {
                fireChildChangedEvents(path, childListeners);
            }
        }

        if (event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted || event.getType() == EventType.NodeCreated) {
            Set<IZkDataListener> listeners = _dataListener.get(path);
            if (listeners != null && !listeners.isEmpty()) {
                fireDataChangedEvents(event.getPath(), listeners);
            }
        }
    }

    private void fireDataChangedEvents(final String path, Set<IZkDataListener> listeners) {
        for (final IZkDataListener listener : listeners) {
            _eventThread.send(new ZkEvent("Data of " + path + " changed sent to " + listener.getClass().getName()) {

                @Override
                public void run() throws Exception {
                    // reinstall watch
                    exists(path, true);
                    try {
                        Serializable data = readData(path, null, true);
                        listener.handleDataChange(path, data);
                    } catch (ZkNoNodeException e) {
                        listener.handleDataDeleted(path);
                    }
                }
            });
        }
    }

    private void fireChildChangedEvents(final String path, Set<IZkChildListener> childListeners) {
        try {
            // reinstall the watch
            for (final IZkChildListener listener : childListeners) {
                _eventThread.send(new ZkEvent("Children of " + path + " changed sent to " + listener.getClass().getName()) {

                    @Override
                    public void run() throws Exception {
                        try {
                            // if the node doesn't exist we should listen for the root node to reappear
                            exists(path);
                            List<String> children = getChildren(path);
                            listener.handleChildChange(path, children);
                        } catch (ZkNoNodeException e) {
                            listener.handleChildChange(path, null);
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("Failed to fire child changed event. Unable to getChildren.  ", e);
        }
    }

    public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) throws ZkInterruptedException {
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
        LOG.debug("Waiting until znode '" + path + "' becomes available.");
        if (exists(path)) {
            return true;
        }
        acquireEventLock();
        try {
            while (!exists(path, true)) {
                boolean gotSignal = getEventLock().getZNodeEventCondition().awaitUntil(timeout);
                if (!gotSignal) {
                    return false;
                }
            }
            return true;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    protected Set<IZkDataListener> getDataListener(String path) {
        return _dataListener.get(path);
    }

    public void showFolders(OutputStream output) {
        final int level = 1;
        final StringBuilder builder = new StringBuilder();
        final String startPath = "/";
        addChildrenToStringBuilder(level, builder, startPath);
        try {
            output.write(builder.toString().getBytes());
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void addChildrenToStringBuilder(final int level, final StringBuilder builder, final String startPath) {
        final List<String> children = getChildren(startPath);
        for (final String node : children) {
            builder.append(getSpaces(level - 1) + "'-" + "+" + node + "\n");

            String nestedPath;
            if (startPath.endsWith("/")) {
                nestedPath = startPath + node;
            } else {
                nestedPath = startPath + "/" + node;
            }

            addChildrenToStringBuilder(level + 1, builder, nestedPath);
        }
    }

    private String getSpaces(final int level) {
        String s = "";
        for (int i = 0; i < level; i++) {
            s += "  ";
        }
        return s;
    }

    public void waitUntilConnected() throws ZkInterruptedException {
        waitUntilConnected(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public boolean waitUntilConnected(long time, TimeUnit timeUnit) throws ZkInterruptedException {
        return waitForKeeperState(KeeperState.SyncConnected, time, timeUnit);
    }

    public boolean waitForKeeperState(KeeperState keeperState, long time, TimeUnit timeUnit) throws ZkInterruptedException {
        if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

        LOG.debug("Waiting for keeper state " + keeperState);
        acquireEventLock();
        try {
            boolean stillWaiting = true;
            while (_currentState != keeperState) {
                if (!stillWaiting) {
                    return false;
                }
                stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeout);
            }
            LOG.debug("State is " + _currentState);
            return true;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    private void acquireEventLock() {
        try {
            getEventLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        }
    }

    /**
     * 
     * @param <T>
     * @param callable
     * @return result of Callable
     * @throws ZkInterruptedException
     *             if operation was interrupted, or a required reconnection got interrupted
     * @throws IllegalArgumentException
     *             if called from anything except the ZooKeeper event thread
     * @throws ZkException
     *             if any ZooKeeper exception occurred
     * @throws RuntimeException
     *             if any other exception occurs from invoking the Callable
     */
    public <T> T retryUntilConnected(Callable<T> callable) throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        while (true) {
            try {
                return callable.call();
            } catch (ConnectionLossException e) {
                // we give the event thread some time to update the status to 'Disconnected'
                Thread.yield();
                waitUntilConnected();
            } catch (SessionExpiredException e) {
                // we give the event thread some time to update the status to 'Expired'
                Thread.yield();
                waitUntilConnected();
            } catch (KeeperException e) {
                throw ZkException.create(e);
            } catch (InterruptedException e) {
                throw new ZkInterruptedException(e);
            } catch (Exception e) {
                throw ExceptionUtil.convertToRuntimeException(e);
            }
        }
    }

    public void setCurrentState(KeeperState currentState) {
        getEventLock().lock();
        try {
            _currentState = currentState;
        } finally {
            getEventLock().unlock();
        }
    }

    /**
     * Returns a mutex all zookeeper events are synchronized aginst. So in case you need to do something without getting
     * any zookeeper event interruption synchronize against this mutex. Also all threads waiting on this mutex object
     * will be notified on an event.
     * 
     * @return the mutex.
     */
    public ZkLock getEventLock() {
        return _zkEventLock;
    }

    public boolean delete(final String path) {
        try {
            retryUntilConnected(new Callable<Object>() {

                @Override
                public Object call() throws Exception {
                    _connection.delete(path);
                    return null;
                }
            });

            return true;
        } catch (ZkNoNodeException e) {
            return false;
        }
    }

    public <T extends Serializable> T readData(String path) {
        return (T) readData(path, null);
    }


    @SuppressWarnings("unchecked")
    public <T extends Serializable> T readData(String path, Stat stat) {
        return (T) readData(path, stat, hasListeners(path));
    }

    @SuppressWarnings("unchecked")
    private <T extends Serializable> T readData(final String path, final Stat stat, final boolean watch) {
        byte[] data = retryUntilConnected(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return _connection.readData(path, stat, watch);
            }
        });
        return (T) readSerializable(data);
    }

    @SuppressWarnings("unchecked")
    private <T extends Serializable> T readSerializable(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(data));
            Object object = inputStream.readObject();
            return (T) object;
        } catch (ClassNotFoundException e) {
            throw new ZkMarshallingError("Unable to find object class.", e);
        } catch (IOException e) {
            throw new ZkMarshallingError(e);
        }
    }

    public void writeData(final String path, Serializable serializable) {
        writeData(path, serializable, -1);
    }

    public void writeData(final String path, Serializable serializable, final int expectedVersion) {
        final byte[] data = toByteArray(serializable);
        retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                _connection.writeData(path, data, expectedVersion);
                return null;
            }
        });
    }

    public void watchForData(final String path) {
        retryUntilConnected(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                _connection.exists(path, true);
                return null;
            }
        });
    }

    /**
     * Installs a child watch for the given path.
     * 
     * @param path
     * @return the current children of the path or null if the zk node with the given path doesn't exist.
     */
    public List<String> watchForChilds(final String path) {
        if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        return retryUntilConnected(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                exists(path, true);
                try {
                    return getChildren(path, true);
                } catch (ZkNoNodeException e) {
                    // ignore, the "exists" watch will listen for the parent node to appear
                }
                return null;
            }
        });
    }

    /**
     * Connect to ZooKeeper.
     * 
     * @param maxMsToWaitUntilConnected
     * @param watcher
     * @throws ZkInterruptedException
     *             if the connection timed out due to thread interruption
     * @throws ZkTimeoutException
     *             if the connection timed out
     * @throws IllegalStateException
     *             if the connection timed out due to thread interruption
     */
    public void connect(final long maxMsToWaitUntilConnected, Watcher watcher) throws ZkInterruptedException, ZkTimeoutException, IllegalStateException {
        boolean started = false;
        try {
            getEventLock().lockInterruptibly();
            setShutdownTrigger(false);
            _eventThread = new ZkEventThread(_connection.getServers());
            _eventThread.start();
            _connection.connect(watcher);

            LOG.debug("Awaiting connection to Zookeeper server");
            if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS)) {
                throw new ZkTimeoutException("Unable to connect to zookeeper server within timeout: " + maxMsToWaitUntilConnected);
            }
            started = true;
        } catch (InterruptedException e) {
            States state = _connection.getZookeeperState();
            throw new IllegalStateException("Not connected with zookeeper server yet. Current state is " + state);
        } finally {
            getEventLock().unlock();

            // we should close the zookeeper instance, otherwise it would keep
            // on trying to connect
            if (!started) {
                close();
            }
        }
    }

    public long getCreationTime(String path) {
        try {
            getEventLock().lockInterruptibly();
            return _connection.getCreateTime(path);
        } catch (KeeperException e) {
            throw ZkException.create(e);
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    /**
     * Close the client. 
     * 
     * @throws ZkInterruptedException
     */
    public void close() throws ZkInterruptedException {
        if (_connection == null) {
            return;
        }
        LOG.debug("Closing ZkClient...");
        getEventLock().lock();
        try {
            setShutdownTrigger(true);
            _eventThread.interrupt();
            _eventThread.join(2000);
            _connection.close();
            _connection = null;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
        LOG.debug("Closing ZkClient...done");
    }

    private void reconnect() {
        getEventLock().lock();
        try {
            _connection.close();
            _connection.connect(this);
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    public void setShutdownTrigger(boolean triggerState) {
        _shutdownTriggered = triggerState;
    }

    public boolean getShutdownTrigger() {
        return _shutdownTriggered;
    }

    public int numberOfListeners() {
        int listeners = 0;
        for (Set<IZkChildListener> childListeners : _childListener.values()) {
            listeners += childListeners.size();
        }
        for (Set<IZkDataListener> dataListeners : _dataListener.values()) {
            listeners += dataListeners.size();
        }
        listeners += _stateListener.size();
        
        return listeners;
    }
}
