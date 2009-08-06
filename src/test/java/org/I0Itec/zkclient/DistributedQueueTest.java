package org.I0Itec.zkclient;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class DistributedQueueTest {

    @Test(timeout = 15000)
    public void testDistributedQueue() throws InterruptedException, IOException, KeeperException {
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDistributedQueue", 4711);
        ZkClient client = zkServer.getZkClient();
        client.createPersistent("/queue");

        DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(client, "/queue");
        distributedQueue.offer(17L);
        distributedQueue.offer(18L);
        distributedQueue.offer(19L);

        assertEquals(Long.valueOf(17L), distributedQueue.poll());
        assertEquals(Long.valueOf(18L), distributedQueue.poll());
        assertEquals(Long.valueOf(19L), distributedQueue.poll());
        assertNull(distributedQueue.poll());

        zkServer.shutdown();
        zkServer.join();
    }

    @Test(timeout = 15000)
    public void testPeek() throws InterruptedException, IOException, KeeperException {
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testPeek", 4711);
        ZkClient client = zkServer.getZkClient();
        client.createPersistent("/queue");

        DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(client, "/queue");
        distributedQueue.offer(17L);
        distributedQueue.offer(18L);

        assertEquals(Long.valueOf(17L), distributedQueue.peek());
        assertEquals(Long.valueOf(17L), distributedQueue.peek());
        assertEquals(Long.valueOf(17L), distributedQueue.poll());
        assertEquals(Long.valueOf(18L), distributedQueue.peek());
        assertEquals(Long.valueOf(18L), distributedQueue.poll());
        assertNull(distributedQueue.peek());

        zkServer.shutdown();
        zkServer.join();
    }

    @Test(timeout = 30000)
    public void testMultipleReadingThreads() throws InterruptedException, IOException, KeeperException {
        ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testDistributedQueue", 4711);
        ZkClient client = zkServer.getZkClient();
        client.createPersistent("/queue");

        final DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(client, "/queue");

        // insert 100 elements
        for (int i = 0; i < 100; i++) {
            distributedQueue.offer(new Long(i));
        }

        // 3 reading threads
        final Set<Long> readElements = Collections.synchronizedSet(new HashSet<Long>());
        List<Thread> threads = new ArrayList<Thread>();
        final List<Exception> exceptions = new Vector<Exception>();

        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Long value = distributedQueue.poll();
                            if (value == null) {
                                return;
                            }
                            readElements.add(value);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        e.printStackTrace();
                    }
                }
            };
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        
        assertEquals(0, exceptions.size());
        assertEquals(100, readElements.size());

        zkServer.shutdown();
        zkServer.join();
    }
}
