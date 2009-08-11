package org.I0Itec.zkclient;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractBaseZkClientTest {

    protected static final Logger LOG = Logger.getLogger(AbstractBaseZkClientTest.class);
    protected ZkServer _zkServer;
    protected ZkClient _client;

    @Before
    public void setUp() throws Exception {
        LOG.info("------------ BEFORE -------------");

    }

    @After
    public void tearDown() throws Exception {
        LOG.info("------------ AFTER -------------");
    }
}
