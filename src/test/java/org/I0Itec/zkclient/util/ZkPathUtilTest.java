package org.I0Itec.zkclient.util;

import junit.framework.TestCase;

import org.I0Itec.zkclient.TestUtil;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

public class ZkPathUtilTest extends TestCase {

    protected ZkServer _zkServer;
    protected ZkClient _client;

    public void testToString() throws Exception {
        _zkServer = TestUtil.startZkServer("ZkPathUtilTest", 4711);
        _client = new ZkClient("localhost:4711", 5000);
        final String file1 = "/files/file1";
        final String file2 = "/files/file2";
        final String file3 = "/files/file2/file3";
        _client.createPersistent(file1, true);
        _client.createPersistent(file2, true);
        _client.createPersistent(file3, true);

        String stringRepresentation = ZkPathUtil.toString(_client);
        System.out.println(stringRepresentation);
        System.out.println("-------------------------");
        assertTrue(stringRepresentation.contains("file1"));
        assertTrue(stringRepresentation.contains("file2"));
        assertTrue(stringRepresentation.contains("file3"));

        // path filtering
        stringRepresentation = ZkPathUtil.toString(_client, "/", new ZkPathUtil.PathFilter() {
            @Override
            public boolean showChilds(String path) {
                return !file2.equals(path);
            }
        });
        assertTrue(stringRepresentation.contains("file1"));
        assertTrue(stringRepresentation.contains("file2"));
        assertFalse(stringRepresentation.contains("file3"));

        // start path
        stringRepresentation = ZkPathUtil.toString(_client, file2, ZkPathUtil.PathFilter.ALL);
        assertFalse(stringRepresentation.contains("file1"));
        assertTrue(stringRepresentation.contains("file2"));
        assertTrue(stringRepresentation.contains("file3"));

        _zkServer.shutdown();
    }
}
