/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZkStateChangeTest {

    private StateOnlyConnection zkConn;
    private ZkClient client;
    private TestStateListener listener;

    @Before
    public void setUp() {
        zkConn = new StateOnlyConnection();
        client = new ZkClient(zkConn);
        listener = new TestStateListener();
        client.subscribeStateChanges(listener);
    }

    @After
    public void tearDown() {
        client.close();
    }

    @Test
    public void testNewSessionEvent() throws Exception {
        zkConn.expireSession();
        assertTimed(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return listener.expiredEvents;
            }
        });

        assertTimed(0, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return listener.sessionEstablishErrors;
            }
        });

        assertTimed(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return listener.newSessionEvent;
            }
        });
    }

    @Test
    public void testFailConnectEvent() throws Exception {
        zkConn.setFailOnConnect(true);
        zkConn.expireSession();
        assertTimed(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return listener.expiredEvents;
            }
        });

        assertTimed(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return listener.sessionEstablishErrors;
            }
        });

        assertTimed(0, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return listener.newSessionEvent;
            }
        });

        client.close();
    }

    private <T> void assertTimed(T expectedVal, Callable<T> condition) throws Exception {
        assertEquals(expectedVal, TestUtil.waitUntil(expectedVal, condition, TimeUnit.SECONDS, 5));
    }

    private static class StateOnlyConnection implements IZkConnection {
        private Watcher _watcher;
        private boolean failOnConnect = false;

        @Override
        public void connect(Watcher w) {
            _watcher = w;
            if (failOnConnect) {
                // As as example:
                throw new RuntimeException("Testing connection failure");
            }
            new Thread() {
                @Override
                public void run() {
                    _watcher.process(new WatchedEvent(null, KeeperState.SyncConnected, null));
                }
            }.start();
        }

        public void expireSession() {
            _watcher.process(new WatchedEvent(null, KeeperState.Expired, null));
        }

        public void setFailOnConnect(boolean failFlag) {
            this.failOnConnect = failFlag;
        }

        @Override
        public void close() throws InterruptedException {

        }

        @Override
        public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void delete(String path) throws InterruptedException, KeeperException {
            throw new RuntimeException("not implemented");
        }
        
        @Override
        public void delete(String path, int version) throws InterruptedException, KeeperException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public States getZookeeperState() {
            throw new RuntimeException("not implemented");
        }

        @Override
        public long getCreateTime(String path) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public String getServers() {
            return "test";
        }

        @Override
        public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addAuthInfo(String scheme, byte[] auth) {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void setAcl(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map.Entry<List<ACL>, Stat> getAcl(String path) throws KeeperException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestStateListener implements IZkStateListener {
        public int expiredEvents = 0;
        public int newSessionEvent = 0;
        public int sessionEstablishErrors = 0;

        @Override
        public void handleStateChanged(KeeperState state) throws Exception {
            if (state == KeeperState.Expired) {
                expiredEvents++;
            }
        }

        @Override
        public void handleNewSession() throws Exception {
            newSessionEvent++;
        }

        @Override
        public void handleSessionEstablishmentError(final Throwable error) throws Exception {
            sessionEstablishErrors++;
        }
    }

}
