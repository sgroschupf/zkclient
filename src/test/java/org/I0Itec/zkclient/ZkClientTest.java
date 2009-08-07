package org.I0Itec.zkclient;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class ZkClientTest extends BaseZkClientTest {

    @Override
    public void setUp() throws InterruptedException, IOException, KeeperException {
        super.setUp();
        _zkServer = TestUtil.startZkServer("ZkClientTest-testWriteAndRead", 4711);
        _client = _zkServer.getZkClient();
    }

    @Override
    public void tearDown() throws InterruptedException {
        super.tearDown();
        _client.close();
        _zkServer.shutdown();
    }
}
