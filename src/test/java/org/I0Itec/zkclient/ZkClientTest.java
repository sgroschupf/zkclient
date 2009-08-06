package org.I0Itec.zkclient;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

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
        zkServer.join();
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
        zkServer.join();
    }

    @Test
    public void testDeleteRecursive() throws Exception {
        LOG.info("--- testDeleteRecursive");
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDeleteRecursive", 4711);
        ZkClient zkClient = zkServer.getZkClient();

        // should be able to call this on a not existing directory
        zkClient.deleteRecursive("/doesNotExist");

        zkServer.shutdown();
        zkServer.join();
    }

    @Test(timeout = 15000)
    public void testRetryUntilConnected() throws Exception {
        LOG.info("--- testRetryUntilConnected");
        final ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testRetryUntilConnected", 4711);
        final ZkConnection connection = new ZkConnection("localhost:4711");
        final ZkClient client = new ZkClient(connection);

        zkServer.shutdown();
        zkServer.join();

        // start server in 250ms
        new DeferredZookeeperStarter(zkServer, 250).start();

        // this should work as soon as the connection is reestablished, if it
        // fails it throws a ConnectionLossException
        client.retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                connection.exists("/a", false);
                return null;
            }
        });

        client.close();
        zkServer.shutdown();
        zkServer.join();
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
        zkServer.join();
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
        zkServer.join();
    }

    @Test
    public void testDataChanges() throws Exception {
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testWaitUntilExists", 4711);
        final ZkClient zkClient = zkServer.getZkClient();
        String path = "/a";
        final Holder<String> holder = new Holder<String>();

        IZkDataListener<String> listener = new IZkDataListener<String>() {

            public void handleDataDeleted(String dataPath) {
                holder.set(null);
            }

            public void handleDataChange(String dataPath, String data) {
                holder.set(data);
            }

            public void handleDataAdded(String dataPath, String data) {
                holder.set(data);
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
        zkServer.join();
    }
}
