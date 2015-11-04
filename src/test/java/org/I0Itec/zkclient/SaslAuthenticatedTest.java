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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class SaslAuthenticatedTest {
    protected ZkClient _client;
    protected ZkServer _zkServer;
    protected static final Logger LOG = Logger.getLogger(AbstractAuthTest.class);
    static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";
    static final String ZK_AUTH_PROVIDER = "zookeeper.authProvider.1";
    final String zkServerContextName = "Server";
    final String zkClientContextName = "Client";
    final String userSuperPasswd = "adminpasswd";
    final String user = "fpj";
    final String userPasswd = "fpjsecret";
    final String zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule";
    
    private String createJaasFile() throws IOException {
        File jaasFile = File.createTempFile("jaas", "conf");
        FileOutputStream jaasOutputStream = new java.io.FileOutputStream(jaasFile);
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", zkServerContextName, zkModule).getBytes());
        jaasOutputStream.write(String.format("\tuser_super=\"%s\"\n", userSuperPasswd).getBytes());
        jaasOutputStream.write(String.format("\tuser_%s=\"%s\";\n};\n\n", user, userPasswd).getBytes());
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", zkClientContextName, zkModule).getBytes());
        jaasOutputStream.write(String.format("\tusername=\"%s\"\n", user).getBytes());
        jaasOutputStream.write(String.format("\tpassword=\"%s\";\n};", userPasswd).getBytes());
        jaasOutputStream.close();
        jaasFile.deleteOnExit();
        return jaasFile.getAbsolutePath();
    }

    @Before
    public void setUp() throws Exception {
        String jaasFileName = createJaasFile();
        LOG.info("Jaas file name " + jaasFileName);
        System.setProperty(JAVA_LOGIN_CONFIG_PARAM, jaasFileName);
        System.setProperty(ZK_AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        _zkServer = TestUtil.startZkServer("ZkClientTest", 4711);
        _client = new ZkClient(new ZkConnection("localhost:4711", 6000), 6000, true);
    }

    @After
    public void tearDown() throws Exception {
        if(_client != null) {
            _client.close();
        }
        _zkServer.shutdown();
        System.clearProperty(ZK_AUTH_PROVIDER);
        System.clearProperty(JAVA_LOGIN_CONFIG_PARAM);
    }
    
    @Test
    public void testConnection() {
        _client.createPersistent("/test", new byte[0], Ids.CREATOR_ALL_ACL);
        Assert.assertTrue(_client.exists("/test"));
    }
}
