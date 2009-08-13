package org.I0Itec.zkclient;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZkClientTest {

    private static final Logger LOG = Logger.getLogger(ZkClientTest.class);

    @Before
    public void setUp() {
        LOG.info("------------ BEFORE -------------");
    }

    @After
    public void tearDown() {
        LOG.info("------------ AFTER -------------");
    }

    @Test(expected = IOException.class, timeout = 5000)
    public void testUnableToConnect() throws Exception {
        LOG.info("--- testUnableToConnect");
        // we are using port 4711 to avoid conflicts with the zk server that is
        // started by the Spring context
        new ZkClient("localhost:4711", 1000);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        LOG.info("--- testWriteAndRead");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testWriteAndRead", 4711);
        ZkClient zkClient = zkServer.getZkClient();
        String data = "something";
        String path = "/a";
        zkClient.createPersistent(path, data);
        String data2 = zkClient.readData(path);
        Assert.assertEquals(data, data2);
        zkClient.delete(path);

        zkServer.shutdown();
    }

    @Test
    public void testDelete() throws Exception {
        LOG.info("--- testDelete");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDelete", 4711);
        ZkClient zkClient = zkServer.getZkClient();
        String path = "/a";
        assertFalse(zkClient.delete(path));
        zkClient.createPersistent(path, null);
        assertTrue(zkClient.delete(path));
        assertFalse(zkClient.delete(path));

        zkServer.shutdown();
    }

    @Test
    public void testDeleteRecursive() throws Exception {
        LOG.info("--- testDeleteRecursive");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDeleteRecursive", 4711);
        ZkClient zkClient = zkServer.getZkClient();

        // should be able to call this on a not existing directory
        zkClient.deleteRecursive("/doesNotExist");

        zkServer.shutdown();
    }

    @Test(timeout = 15000)
    public void testRetryUntilConnected() throws Exception {
        LOG.info("--- testRetryUntilConnected");
        final ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testRetryUntilConnected", 4711);
        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        final ZkConnection zkConnection = new ZkConnection("localhost:4712");
        final ZkClient zkClient = new ZkClient(zkConnection, 5000);

        gateway.stop();

        // start server in 250ms
        new DeferredGatewayStarter(gateway, 250).start();

        // this should work as soon as the connection is reestablished, if it
        // fails it throws a ConnectionLossException
        zkClient.retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                zkConnection.exists("/a", false);
                return null;
            }
        });

        zkClient.close();
        zkServer.shutdown();
        gateway.stop();
    }

    @Test(timeout = 15000)
    public void testWaitUntilConnected() throws Exception {
        LOG.info("--- testWaitUntilConnected");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testWaitUntilConnected", 4711);

        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        ZkClient client = new ZkClient("localhost:4712", 5000);

        // interrupt the connection
        gateway.stop();

        // the client state should change to KeeperState.Disconnected
        assertTrue(client.waitForKeeperState(KeeperState.Disconnected, 1, TimeUnit.SECONDS));

        // connection should not be possible and timeout after 100ms
        assertFalse(client.waitUntilConnected(100, TimeUnit.MILLISECONDS));

        client.close();
        zkServer.shutdown();
    }

    @Test
    public void testWaitUntilExists() throws InterruptedException, IOException, KeeperException {
        LOG.info("--- testWaitUntilExists");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testWaitUntilExists", 4711);
        final ZkClient zkClient = zkServer.getZkClient();

        // create /gaga node asynchronously
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                    zkClient.createPersistent("/gaga");
                } catch (Exception e) {
                    // ignore
                }
            }
        }.start();

        // wait until this was created
        assertTrue(zkClient.waitUntilExists("/gaga", TimeUnit.SECONDS, 5));
        assertTrue(zkClient.exists("/gaga"));

        // waiting for /neverCreated should timeout
        assertFalse(zkClient.waitUntilExists("/neverCreated", TimeUnit.MILLISECONDS, 100));

        zkServer.shutdown();
    }

    @Test
    public void testDataChanges1() throws Exception {
        LOG.info("--- testDataChanges1");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDataChanges1", 4711);
        final ZkClient zkClient = zkServer.getZkClient();
        String path = "/a";
        final Holder<String> holder = new Holder<String>();

        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataChange(String dataPath, Serializable data) throws Exception {
                holder.set((String) data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                holder.set(null);
            }
        };
        zkClient.subscribeDataChanges(path, listener);
        zkClient.createPersistent(path, "aaa");

        // wait some time to make sure the event was triggered
        String contentFromHolder = TestUtil.waitUntil("b", new Callable<String>() {

            @Override
            public String call() throws Exception {
                return holder.get();
            }
        }, TimeUnit.SECONDS, 5);

        assertEquals("aaa", contentFromHolder);

        zkServer.shutdown();
    }

    @Test(timeout = 15000)
    public void testRetryUntilConnected_SessionExpiredException() throws InterruptedException, IOException, KeeperException {
        LOG.info("--- testRetryUntilConnected_SessionExpiredException");

        // Use a tick time of 100ms, because the minimum session timeout is 2 x tick-time.
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testSessionExpiredException", 4711, 100);
        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();

        // Use a session timeout of 200ms
        final ZkClient zkClient = new ZkClient("localhost:4712", 200, 5000);

        gateway.stop();

        // Start server in 600ms, the session should have expired by then
        new DeferredGatewayStarter(gateway, 600).start();

        // This should work as soon as a new session has been created (and the connection is reestablished), if it fails it throws a SessionExpiredException
        zkClient.retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                zkClient.exists("/a");
                return null;
            }
        });

        zkClient.close();
        zkServer.shutdown();
        gateway.stop();
    }

    @Test(timeout = 15000)
    public void testChildListenerAfterSessionExpiredException() throws Exception {
        LOG.info("--- testChildListenerAfterSessionExpiredException");

        int sessionTimeout = 200;
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testChildListenerAfterSessionExpiredException", 4711, sessionTimeout / 2);
        ZkClient connectedClient = zkServer.getZkClient();
        connectedClient.createPersistent("/root");

        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();

        final ZkClient disconnectedZkClient = new ZkClient("localhost:4712", sessionTimeout, 5000);
        final Holder<List<String>> children = new Holder<List<String>>();
        disconnectedZkClient.subscribeChildChanges("/root", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                children.set(currentChilds);
            }
        });

        gateway.stop();

        // The connected client now created a new child node
        connectedClient.createPersistent("/root/node");

        // Wait for 3 x sessionTimeout, the session should have expired by then and start the gateway again
        Thread.sleep(sessionTimeout * 3);
        gateway.start();

        Boolean hasOneChild = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return children.get() != null && children.get().size() == 1;
            }
        }, TimeUnit.SECONDS, 5);

        assertTrue(hasOneChild);

        disconnectedZkClient.close();
        zkServer.shutdown();
        gateway.stop();
    }

    @Test
    public void testDataChanges2() throws Exception {
        LOG.info("--- testDataChanges2");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDataChanges2", 4711);
        final ZkClient zkClient = zkServer.getZkClient();
        String path = "/a";
        final AtomicInteger countChanged = new AtomicInteger(0);
        final AtomicInteger countDeleted = new AtomicInteger(0);

        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataChange(String dataPath, Serializable data) throws Exception {
                countChanged.incrementAndGet();
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                countDeleted.incrementAndGet();
            }
        };
        zkClient.subscribeDataChanges(path, listener);

        // create node
        zkClient.createPersistent(path, "aaa");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countChanged.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, countChanged.get());
        assertEquals(0, countDeleted.get());

        countChanged.set(0);
        countDeleted.set(0);
        // delete node, this should trigger a delete event
        zkClient.delete(path);
        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countDeleted.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(0, countChanged.get());
        assertEquals(1, countDeleted.get());

        // test if watch was reinstalled after the file got deleted
        countChanged.set(0);
        zkClient.createPersistent(path, "aaa");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countChanged.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, countChanged.get());

        // test if changing the contents notifies the listener
        zkClient.writeData(path, "bbb");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(2, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countChanged.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(2, countChanged.get());

        zkServer.shutdown();
    }

    @Test(timeout = 15000)
    public void testHandleChildChanges() throws Exception {
        LOG.info("--- testHandleChildChanges");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testHandleChildChanges", 4711);
        final ZkClient zkClient = zkServer.getZkClient();
        String path = "/a";
        final AtomicInteger count = new AtomicInteger(0);
        final Holder<List<String>> children = new Holder<List<String>>();

        IZkChildListener listener = new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                count.incrementAndGet();
                children.set(currentChilds);
            }
        };
        zkClient.subscribeChildChanges(path, listener);

        // ----
        // Create the root node should throw the first child change event
        // ----
        zkClient.createPersistent(path);

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return count.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, count.get());
        assertEquals(0, children.get().size());

        // ----
        // Creating a child node should throw another event
        // ----
        count.set(0);
        zkClient.createPersistent(path + "/child1");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return count.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, count.get());
        assertEquals(1, children.get().size());
        assertEquals("child1", children.get().get(0));

        // ----
        // Creating another child and deleting the node should also throw an event
        // ----
        count.set(0);
        zkClient.createPersistent(path + "/child2");
        zkClient.deleteRecursive(path);

        // wait some time to make sure the event was triggered
        Boolean eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0 && children.get() == null;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertNull(children.get());

        // ----
        // Creating root again should throw an event
        // ----
        count.set(0);
        zkClient.createPersistent(path);

        // wait some time to make sure the event was triggered
        eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertEquals(0, children.get().size());

        // ----
        // Creating child now should throw an event
        // ----
        count.set(0);
        zkClient.createPersistent(path + "/child");

        // wait some time to make sure the event was triggered
        eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertEquals(1, children.get().size());
        assertEquals("child", children.get().get(0));

        // ----
        // Deleting root node should throw an event
        // ----
        count.set(0);
        zkClient.deleteRecursive(path);

        // wait some time to make sure the event was triggered
        eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0 && children.get() == null;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertNull(children.get());

        zkServer.shutdown();
    }
}
