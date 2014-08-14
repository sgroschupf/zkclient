package org.I0Itec.zkclient;

import org.junit.Test;

import java.io.IOException;

public class TestRemoteZk {

    @Test
    public void test() throws InterruptedException, IOException {
        ZkServer _zkServer = TestUtil.startZkServerWithIP("192.168.1.1", "ZkClientTest_DNS_Change", 2181);

        ZkClient client = new ZkClient("zkjbae:2181", 5000);
        client.createEphemeral("/a");
        for (int i = 0; i < 10; ++i) {
            client.readData("/a");
            client.writeData("/a", Integer.toString(i));
            Thread.sleep(1000);
            System.out.println("sleeping..." + i);
        }
        _zkServer.shutdown();

        _zkServer = TestUtil.startZkServerWithIP("192.168.1.2", "ZkClientTest_DNS_Change", 2181);
        // change /etc/hosts
        client.createEphemeral("/a");
        for (int i = 0; i < 10; ++i) {
            client.readData("/a");
            client.writeData("/a", Integer.toString(i));
            Thread.sleep(1000);
            System.out.println("sleeping..." + i);
        }
        _zkServer.shutdown();
    }
}
