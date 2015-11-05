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

import javax.security.auth.login.Configuration;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class SaslAuthenticatedTest {
    protected static final Logger LOG = Logger.getLogger(SaslAuthenticatedTest.class);
    static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";
    static final String ZK_AUTH_PROVIDER = "zookeeper.authProvider.1";
    static final String ZK_ALLOW_FAILED_SASL = "zookeeper.allowSaslFailedClients";
    public static final String ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";

    private int _port = 4700;
    private ZkClient _client;
    private ZkServer _zkServer;
    private String _zkServerContextName = "Server";
    private String _zkClientContextName = "Client";
    private String _userSuperPasswd = "adminpasswd";
    private String _userServerSide = "fpj";
    private String _userClientSide = "fpj";
    private String _userServerSidePasswd = "fpjsecret";
    private String _userClientSidePasswd = "fpjsecret";
    private String _zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule";

    private String createJaasFile() throws IOException {
        File jaasFile = File.createTempFile("jaas", "conf");
        FileOutputStream jaasOutputStream = new java.io.FileOutputStream(jaasFile);
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", _zkServerContextName, _zkModule).getBytes());
        jaasOutputStream.write(String.format("\tuser_super=\"%s\"\n", _userSuperPasswd).getBytes());
        jaasOutputStream.write(String.format("\tuser_%s=\"%s\";\n};\n\n", _userServerSide, _userServerSidePasswd).getBytes());
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", _zkClientContextName, _zkModule).getBytes());
        jaasOutputStream.write(String.format("\tusername=\"%s\"\n", _userClientSide).getBytes());
        jaasOutputStream.write(String.format("\tpassword=\"%s\";\n};", _userClientSidePasswd).getBytes());
        jaasOutputStream.close();
        jaasFile.deleteOnExit();
        return jaasFile.getAbsolutePath();
    }

    @Before
    public void setUp() throws IOException {
        System.setProperty(ZK_AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");

        // Reset all variables used for the jaas login file
        this._zkServerContextName = "Server";
        this._zkClientContextName = "Client";
        this._userSuperPasswd = "adminpasswd";
        this._userServerSide = "fpj";
        this._userClientSide = "fpj";
        this._userServerSidePasswd = "fpjsecret";
        this._userClientSidePasswd = "fpjsecret";
        this._zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule";
    }

    @After
    public void tearDown() {
        if(_zkServer != null) {
            _zkServer.shutdown();
        }
        System.clearProperty(ZK_AUTH_PROVIDER);
        System.clearProperty(JAVA_LOGIN_CONFIG_PARAM);
    }

    private void bootstrap() throws IOException {
        Configuration.setConfiguration(null);
        String jaasFileName = createJaasFile();
        System.setProperty(JAVA_LOGIN_CONFIG_PARAM, jaasFileName);
        _zkServer = TestUtil.startZkServer("ZkClientTest", _port);
        _client = _zkServer.getZkClient();
    }

    /**
     * Tests that a connection authenticates successfully.
     */
    @Test
    public void testConnection() {
        try {
            bootstrap();
            _client.createPersistent("/test", new byte[0], Ids.CREATOR_ALL_ACL);
            Assert.assertTrue(_client.exists("/test"));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Tests that ZkClient spots the AuthFailed event in the
     * case the property to allow failed SASL connections is
     * enabled.
     */
    @Test
    public void testAuthFailureFailedSasl() {
        System.setProperty(ZK_ALLOW_FAILED_SASL, "true");
        try {
            testAuthFailure();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        } finally {
            System.clearProperty(ZK_ALLOW_FAILED_SASL);
        }
    }

    /**
     * Tests that ZkClient throws an exception in the case ZooKeeper
     * keeps dropping the connection due to authentication failures.
     */
    @Test
    public void testAuthFailureDisconnect() {
        try {
            testAuthFailure();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    private void testAuthFailure() throws IOException {
        _userServerSide = "otheruser";
        String jaasFileName = createJaasFile();
        System.setProperty(JAVA_LOGIN_CONFIG_PARAM, jaasFileName);
        try {
            bootstrap();
            Assert.fail("Should have thrown an exception");
        } catch (ZkException e) {
            // expected
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}
