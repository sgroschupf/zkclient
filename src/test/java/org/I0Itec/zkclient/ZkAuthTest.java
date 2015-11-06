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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZkAuthTest extends AbstractAuthTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        _zkServer = TestUtil.startZkServer("ZkClientTest", 4711);
        _client = new ZkClient("localhost:4711", 25000);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (_client != null) {
            _client.close();
        }
        _zkServer.shutdown();
    }

    @Test
    public void testAuthorized() {
        _client.addAuthInfo("digest", "pat:pass".getBytes());
        _client.create("/path1", null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        _client.readData("/path1");
    }

    @Test
    public void testSetAndGetAcls() {
        _client.addAuthInfo("digest", "pat:pass".getBytes());

        _client.create("/path1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertThat(_client.getAcl("/path1").getKey()).isEqualTo(ZooDefs.Ids.OPEN_ACL_UNSAFE);

        for (int i = 0; i < 100; i++) {
            _client.setAcl("/path1", ZooDefs.Ids.OPEN_ACL_UNSAFE);
            assertThat(_client.getAcl("/path1").getKey()).isEqualTo(ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
    }
}
