package org.I0Itec.zkclient;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class InMemoryConnectionTest {

    @Test
    public void testGetChildren_OnEmptyFileSystem() throws KeeperException, InterruptedException {
        InMemoryConnection connection = new InMemoryConnection();
        List<String> children = connection.getChildren("/", false);
        assertEquals(0, children.size());
    }
}
