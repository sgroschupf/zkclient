package org.I0Itec.zkclient;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * To test this unit testing, the followings should be done first
 * /etc/hosts contains 192.168.1.1 zkjbae
 * 192.168.1.1 and 192.168.1.2 are binded on lo0 interface using
 * sudo ifconfig lo0 192.168.1.2 alias
 * sudo ifconfig lo0 192.168.1.2 alias
 *
 * Running the unit test after shutting down the first _zkServer,
 * /etc/hosts should be fixed with 192.168.1.2 zkjbae to reroute DNS
 */
@Ignore
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
