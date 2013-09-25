package org.I0Itec.zkclient;

import java.net.UnknownHostException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.assertEquals;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

import org.junit.Test;

public class ZkStateChangeTest{

    private StateOnlyConnection zkConn;
    private ZkClient client;
    private TestStateListener listener;

    @Before
    public void setUp(){
        zkConn = new StateOnlyConnection();
        client = new ZkClient(zkConn);
        listener =  new TestStateListener();
        client.subscribeStateChanges(listener);
    }

    @After
    public void tearDown(){
        client.close();
    }

    @Test
    public void testNewSessionEvent() throws Exception{
        zkConn.expireSession();
        assertTimed(1,
                    new Callable<Integer>(){
                        @Override
                        public Integer call() throws Exception{
                            return listener.expiredEvents;
                        }
                    });

        assertTimed(1,
                    new Callable<Integer>(){
                        @Override
                        public Integer call() throws Exception{
                            return listener.newSessionEvent;
                        }
                    });
    }
    @Test
    public void testFailConnectEvent() throws Exception{
        zkConn.setFailOnConnect(true);
        zkConn.expireSession();
        assertTimed(1,
                    new Callable<Integer>(){
                        @Override
                        public Integer call() throws Exception{
                            return listener.expiredEvents;
                        }
                    });

        assertTimed(1,
                    new Callable<Integer>(){
                        @Override
                        public Integer call() throws Exception{
                            return listener.sessionEstablishErrors;
                        }
                    });

        client.close();
    }


    private <T> void assertTimed(T expectedVal, Callable<T> condition) throws Exception{
        assertEquals(expectedVal, TestUtil.waitUntil(expectedVal, condition, TimeUnit.SECONDS, 5));
    }


    private static class StateOnlyConnection implements IZkConnection{
        private Watcher _watcher;
        private boolean failOnConnect = false;
        @Override
        public void connect(Watcher w){
            _watcher = w;
            if(failOnConnect){
                //As as example:
                throw new RuntimeException("Testing connection failure");
            }
            new Thread(){
                @Override
                public void run() {
                    _watcher.process(new WatchedEvent(null, KeeperState.SyncConnected, null));
                }
            }.start();
        }

        public void expireSession(){
            _watcher.process(new WatchedEvent(null, KeeperState.Expired, null));
        }

        public void setFailOnConnect(boolean failFlag){
            this.failOnConnect = failFlag;
        }

        @Override
        public void close() throws InterruptedException{

        }

        @Override
        public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        @Override
        public void delete(String path) throws InterruptedException, KeeperException{
            throw new RuntimeException("not implemented");
        }
        @Override
        public boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        @Override
        public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        @Override
        public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        @Override
        public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        @Override
        public States getZookeeperState(){
            throw new RuntimeException("not implemented");
        }

        @Override
        public long getCreateTime(String path) throws KeeperException, InterruptedException{
            throw new RuntimeException("not implemented");
        }

        @Override
        public String getServers(){
            return "test";
        }
    }

    private static class TestStateListener implements IZkStateListener{
        public int expiredEvents = 0;
        public int newSessionEvent = 0;
        public int sessionEstablishErrors = 0;

        @Override
        public void handleStateChanged(KeeperState state) throws Exception{
            if(state == KeeperState.Expired) {
                System.out.println("HANDLING STATE CHAGED");
                expiredEvents++;
            }
        }
        @Override
        public void handleNewSession() throws Exception{
            System.out.println("HANDLING STATE CHAGED");
            newSessionEvent++;
        }
        @Override
        public void handleSessionEstablishmentError(final Throwable error) throws Exception{
            sessionEstablishErrors++;
        }
    }


}
