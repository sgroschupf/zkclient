package org.I0Itec.zkclient;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContentWatcherTest {

    private static final String FILE_NAME = "/ContentWatcherTest";
    private ZkServer _zkServer;
    private ZkClient _zkClient;

    @Before
    public void setUp() throws Exception {
        _zkServer = ZkTestUtil.startZkServer("ContentWatcherTest", 4711);
        _zkClient = _zkServer.getZkClient();
    }

    @After
    public void tearDown() throws Exception {
        if (_zkServer != null) {
            _zkServer.shutdown();
            _zkServer.join();
        }
    }

    @Test
    public void testGetContent() throws InterruptedException, KeeperException, IOException {
        _zkClient.createPersistent(FILE_NAME, "a");
        ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
        watcher.start();
        assertEquals("a", watcher.getContent());

        // update the content
        _zkClient.writeData(FILE_NAME, "b");
        Thread.sleep(200);
        assertEquals("b", watcher.getContent());
        watcher.stop();
    }

    @Test
    public void testGetContentWaitTillCreated() throws InterruptedException, KeeperException, IOException {
        final Holder<String> contentHolder = new Holder<String>();

        Thread thread = new Thread() {
            @Override
            public void run() {
                ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
                try {
                    watcher.start();
                    contentHolder.set(watcher.getContent());
                    watcher.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        thread.start();

        // create content after 200ms
        Thread.sleep(200);
        _zkClient.createPersistent(FILE_NAME, "aaa");

        // we give the thread some time to pick up the change
        thread.join(1000);
        assertEquals("aaa", contentHolder.get());
    }

    @Test
    public void testHandlingNullContent() throws InterruptedException, KeeperException, IOException {
        _zkClient.createPersistent(FILE_NAME, null);
        ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
        watcher.start();
        assertEquals(null, watcher.getContent());
    }

    @Test(timeout = 15000)
    public void testHandlingOfConnectionLoss() throws Exception {
        final ZkClient zkClient = new ZkClient("localhost:4711", 5000);

        _zkServer.shutdown();
        _zkServer.join();

        // start server in 250ms
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(250);
                    _zkServer.start();
                    zkClient.createPersistent(FILE_NAME, "aaa");
                } catch (Exception e) {
                    // ignore
                }
            }
        }.start();

        ContentWatcher<String> watcher = new ContentWatcher<String>(zkClient, FILE_NAME);
        watcher.start();
        assertEquals("aaa", watcher.getContent());
        watcher.stop();
        
        zkClient.close();
    }
}
