/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractAuthTest {
    protected ZkClient _client;
    protected ZkServer _zkServer;
    protected static final Logger LOG = Logger.getLogger(AbstractAuthTest.class);

    @Before
    public void setUp() throws Exception {
        LOG.info("------------ BEFORE -------------");

    }

    @After
    public void tearDown() throws Exception {
        LOG.info("------------ AFTER -------------");
    }

    @Test
    public void testBadAuth() {
        try {
            List<ACL> acl = new ArrayList<ACL>();
            acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", "pat:pass")));
            _client.create("/path1", null, acl, CreateMode.EPHEMERAL);
            _client.addAuthInfo("digest", "pat:pass2".getBytes());
            _client.readData("/path1");
            fail("Should get auth error");
        } catch (ZkException e) {
            if (e.getCause() instanceof KeeperException && ((KeeperException) e.getCause()).code() == KeeperException.Code.NOAUTH) {
                // do nothing, this is expected
            } else {
                fail("wrong exception");
            }
        }
    }

}
