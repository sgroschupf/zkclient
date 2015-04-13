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
        _client = new ZkClient("localhost:4711", 5000);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        _client.close();
        _zkServer.shutdown();
    }

    @Test
    public void testAuthorized() {
        _client.addAuthInfo("digest", "pat:pass".getBytes());
        _client.create("/path1", null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        _client.readData("/path1");
    }
}
