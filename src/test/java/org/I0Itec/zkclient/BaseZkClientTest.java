package org.I0Itec.zkclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class BaseZkClientTest {

    protected static final Logger LOG = Logger.getLogger(BaseZkClientTest.class);
    protected ZkServer _zkServer;
    protected ZkClient _client;

    @Before
    public void setUp() throws InterruptedException, IOException, KeeperException {
        LOG.info("------------ BEFORE -------------");

    }

    @After
    public void tearDown() throws InterruptedException {
        LOG.info("------------ AFTER -------------");

    }

    @Test(expected = IOException.class, timeout = 5000)
    public void testUnableToConnect() throws Exception {
        LOG.info("--- testUnableToConnect");
        // trying to a non existing zk server
        new ZkClient("localhost:4799", 1000);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        LOG.info("--- testWriteAndRead");

        String data = "something";
        String path = "/a";
        _client.createPersistent(path, data);
        String data2 = _client.readData(path);
        Assert.assertEquals(data, data2);
        _client.delete(path);

    }

    @Test
    public void testDelete() throws Exception {
        LOG.info("--- testDelete");
        String path = "/a";
        assertFalse(_client.delete(path));
        _client.createPersistent(path, null);
        assertTrue(_client.delete(path));
        assertFalse(_client.delete(path));
    }

    @Test
    public void testDeleteRecursive() throws Exception {
        LOG.info("--- testDeleteRecursive");
        // should be able to call this on a not existing directory
        _client.deleteRecursive("/doesNotExist");
    }

    @Test
    public void testWaitUntilExists() throws InterruptedException, IOException, KeeperException {
        LOG.info("--- testWaitUntilExists");

        // create /gaga node asynchronously
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                    _client.createPersistent("/gaga");
                } catch (Exception e) {
                    // ignore
                }
            }
        }.start();

        // wait until this was created
        assertTrue(_client.waitUntilExists("/gaga", TimeUnit.SECONDS, 5));
        assertTrue(_client.exists("/gaga"));

        // waiting for /neverCreated should timeout
        assertFalse(_client.waitUntilExists("/neverCreated", TimeUnit.MILLISECONDS, 100));
    }
}
