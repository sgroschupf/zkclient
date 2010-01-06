package org.I0Itec.zkclient;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.testutil.ZkTestSystem;
import org.junit.Rule;
import org.junit.Test;

public class ZkClientSerializationTest {

    @Rule
    public ZkTestSystem _zk = ZkTestSystem.getInstance();

    @Test
    public void testBytes() throws Exception {
        ZkClient zkClient = new ZkClient(_zk.getZkServerAddress(), 2000, 2000, new BytesPushThroughSerializer());
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        zkClient.createPersistent("/a", bytes);
        byte[] readBytes = zkClient.readData("/a");
        assertArrayEquals(bytes, readBytes);
    }

    @Test
    public void testSerializables() throws Exception {
        ZkClient zkClient = new ZkClient(_zk.getZkServerAddress(), 2000, 2000, new SerializableSerializer());
        String data = "hello world";
        zkClient.createPersistent("/a", data);
        String readData = zkClient.readData("/a");
        assertEquals(data, readData);
    }
}
