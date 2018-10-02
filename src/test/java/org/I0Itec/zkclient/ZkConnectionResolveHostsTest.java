package org.I0Itec.zkclient;

import org.I0Itec.zkclient.testutil.ZkTestSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ZkConnectionResolveHostsTest {
    @Rule
    public ZkTestSystem _zk = ZkTestSystem.getInstance();

    @Test
    public void ZkConnectionResolveHosts() {
        String connectionString = "host-unknown,localhost:" + ZkTestSystem.getInstance().getZkServer().getPort();
        IZkConnection connection = new ZkConnection(connectionString);
        new ZkClient(connection);

        String connectionStringWithZkRoot = "host-unknown:5070,localhost:" + ZkTestSystem.getInstance().getZkServer().getPort() + "/zkroot";
        IZkConnection connectionWithZkRoot = new ZkConnection(connectionStringWithZkRoot);
        new ZkClient(connectionWithZkRoot);

        String connectionStringWithSingleHost = "localhost:" + ZkTestSystem.getInstance().getZkServer().getPort();
        IZkConnection connectionWithSingleHost = new ZkConnection(connectionStringWithSingleHost);
        new ZkClient(connectionWithSingleHost);
        Assert.assertEquals("localhost:" + ZkTestSystem.getInstance().getZkServer().getPort(), connectionWithSingleHost.getServers());
    }

}
