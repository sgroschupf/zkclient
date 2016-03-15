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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.security.auth.login.Configuration;

import org.I0Itec.zkclient.exception.ZkAuthFailedException;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SaslAuthenticatedTest {
    protected static final Logger LOG = Logger.getLogger(SaslAuthenticatedTest.class);
    static final String ZK_AUTH_PROVIDER = "zookeeper.authProvider.1";
    static final String ZK_ALLOW_FAILED_SASL = "zookeeper.allowSaslFailedClients";

    @Rule
    public TemporaryFolder _temporaryFolder = new TemporaryFolder();
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
        File jaasFile = _temporaryFolder.newFile("jaas.conf");
        FileOutputStream jaasOutputStream = new java.io.FileOutputStream(jaasFile);
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", _zkServerContextName, _zkModule).getBytes());
        jaasOutputStream.write(String.format("\tuser_super=\"%s\"\n", _userSuperPasswd).getBytes());
        jaasOutputStream.write(String.format("\tuser_%s=\"%s\";\n};\n\n", _userServerSide, _userServerSidePasswd).getBytes());
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", _zkClientContextName, _zkModule).getBytes());
        jaasOutputStream.write(String.format("\tusername=\"%s\"\n", _userClientSide).getBytes());
        jaasOutputStream.write(String.format("\tpassword=\"%s\";\n};", _userClientSidePasswd).getBytes());
        jaasOutputStream.close();
        return jaasFile.getAbsolutePath();
    }

    @Before
    public void setUp() throws IOException {
        // Reset all variables used for the jaas login file
        _zkServerContextName = "Server";
        _zkClientContextName = "Client";
        _userSuperPasswd = "adminpasswd";
        _userServerSide = "fpj";
        _userClientSide = "fpj";
        _userServerSidePasswd = "fpjsecret";
        _userClientSidePasswd = "fpjsecret";
        _zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule";
    }

    @After
    public void tearDown() {
        if (_client != null) {
            _client.close();
        }
        if (_zkServer != null) {
            _zkServer.shutdown();
        }
        System.clearProperty(ZK_AUTH_PROVIDER);
        System.clearProperty(ZkClient.JAVA_LOGIN_CONFIG_PARAM);
        Configuration.setConfiguration(null);
    }

    private void bootstrap() throws IOException {
        Configuration.setConfiguration(null);
        String jaasFileName = createJaasFile();
        System.setProperty(ZK_AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty(ZkClient.JAVA_LOGIN_CONFIG_PARAM, jaasFileName);
        _zkServer = TestUtil.startZkServer(_temporaryFolder, _port);
        _client = _zkServer.getZkClient();
    }

    private void bootstrapWithAuthFailure() throws IOException {
        _userServerSide = "otheruser";
        bootstrap();
    }

    /**
     * Tests that a connection authenticates successfully.
     * 
     * @throws IOException
     */
    @Test
    public void testConnection() throws IOException {
        bootstrap();
        _client.createPersistent("/test", new byte[0], Ids.CREATOR_ALL_ACL);
        assertThat(_client.exists("/test")).isTrue();
    }

    /**
     * Tests that ZkClient throws an exception in the case ZooKeeper keeps dropping the connection due to authentication
     * failures.
     * 
     * @throws IOException
     */
    @Test
    public void testAuthFailure() throws IOException {
        try {
            bootstrapWithAuthFailure();
            fail("Expected to fail!");
        } catch (ZkException e) {
            assertThat(e).isInstanceOf(ZkTimeoutException.class);
        }
    }

    /**
     * Tests that ZkClient spots the AuthFailed event in the case the property to allow failed SASL connections is
     * enabled.
     * 
     * @throws IOException
     */
    @Test
    public void testAuthFailure_AllowFailedSasl() throws IOException {
        System.setProperty(ZK_ALLOW_FAILED_SASL, "true");
        try {
            bootstrapWithAuthFailure();
            fail("Expected to fail!");
        } catch (ZkException e) {
            assertThat(e).isInstanceOf(ZkAuthFailedException.class);
        } finally {
            System.clearProperty(ZK_ALLOW_FAILED_SASL);
        }
    }

    /**
     * Tests that ZkClient spots the AuthFailed event in the case the property to allow failed SASL connections is
     * enabled.
     * 
     * @throws IOException
     */
    @Test
    public void testAuthFailure_DisabledSasl() throws IOException {
        System.setProperty(ZkClient.ZK_SASL_CLIENT, "false");
        try {
            bootstrapWithAuthFailure();
        } finally {
            System.clearProperty(ZkClient.ZK_SASL_CLIENT);
        }
    }

    @Test
    public void testUnauthenticatedClient() throws IOException {
        ZkClient unauthed = null;
        try {
            bootstrap();
            System.clearProperty(ZkClient.JAVA_LOGIN_CONFIG_PARAM);
            System.setProperty("zookeeper.sasl.client", "true");
            unauthed = new ZkClient("localhost:" + _port, 6000);
            unauthed.createPersistent("/test", new byte[0], Ids.OPEN_ACL_UNSAFE);
        } finally {
            if (unauthed != null) {
                unauthed.close();
            }
        }
    }
    
    @Test
    public void testNoZkJaasFile() throws IOException {
        try {
            _zkClientContextName = "OtherClient";
            _zkServerContextName = "OtherServer";
            bootstrap();
            _client.createPersistent("/test", new byte[0], Ids.OPEN_ACL_UNSAFE);
            assertThat(_client.exists("/test")).isTrue();
        } catch (ZkAuthFailedException e) {
            fail("Caught ZkAuthFailed exception and was not expecting it");
        }
    }
}
