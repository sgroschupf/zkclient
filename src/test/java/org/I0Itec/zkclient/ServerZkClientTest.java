package org.I0Itec.zkclient;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

public class ServerZkClientTest extends BaseZkClientTest {
	private AtomicInteger _counter = new AtomicInteger();
	@Before
	public void setUp() throws InterruptedException, IOException,
			KeeperException {
		super.setUp();
		_zkServer = TestUtil.startZkServer("ZkClientTest_"
				+ _counter.addAndGet(1), 4711);
		_client = new ZkClient("localhost:4711", 5000);
	}

	@After
	public void tearDown() throws InterruptedException {
		super.tearDown();
		_client.close();
		_zkServer.shutdown();
	}
}
