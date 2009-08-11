package org.I0Itec.zkclient;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServerZkClientTest extends AbstractBaseZkClientTest {
    private AtomicInteger _counter = new AtomicInteger();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        _zkServer = TestUtil.startZkServer("ZkClientTest_" + _counter.addAndGet(1), 4711);
        _client = new ZkClient("localhost:4711", 5000);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        _client.close();
        _zkServer.shutdown();
    }

    @Test(timeout = 15000)
    public void testRetryUntilConnected() throws Exception {
        LOG.info("--- testRetryUntilConnected");
        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        final ZkConnection zkConnection = new ZkConnection("localhost:4712");
        final ZkClient zkClient = new ZkClient(zkConnection, 1000);

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
        gateway.stop();
    }

    @Test(timeout = 15000)
    public void testWaitUntilConnected() throws Exception {
        LOG.info("--- testWaitUntilConnected");
        ZkClient _client = new ZkClient("localhost:4711", 5000);

        _zkServer.shutdown();

        // the _client state should change to KeeperState.Disconnected
        assertTrue(_client.waitForKeeperState(KeeperState.Disconnected, 1, TimeUnit.SECONDS));

        // connection should not be possible and timeout after 100ms
        assertFalse(_client.waitUntilConnected(100, TimeUnit.MILLISECONDS));
    }

}
