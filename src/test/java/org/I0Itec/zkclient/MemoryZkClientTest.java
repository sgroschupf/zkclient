package org.I0Itec.zkclient;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

public class MemoryZkClientTest extends BaseZkClientTest {

    @Override
    public void setUp() throws InterruptedException, IOException, KeeperException {
        super.setUp();
        _client = new ZkClient(new InMemoryConnection());
    }

    @Override
    public void tearDown() throws InterruptedException {
        super.tearDown();
        _client.close();
    }

    @Test
    public void testGetChildren() throws Exception {
        String path1 = "/a";
        String path2 = "/a/a";
        String path3 = "/a/a/a";

        _client.create(path1, null, CreateMode.PERSISTENT);
        _client.create(path2, null, CreateMode.PERSISTENT);
        _client.create(path3, null, CreateMode.PERSISTENT);
        Assert.assertEquals(1, _client.getChildren(path1).size());
        Assert.assertEquals(1, _client.getChildren(path2).size());
        Assert.assertEquals(0, _client.getChildren(path3).size());
    }
}
