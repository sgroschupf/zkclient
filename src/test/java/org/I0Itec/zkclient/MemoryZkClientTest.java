package org.I0Itec.zkclient;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class MemoryZkClientTest extends BaseZkClientTest {

	@Override
	public void setUp() throws InterruptedException, IOException,
			KeeperException {
		super.setUp();
		_client = new ZkClient(new InMemoryConnection());
	}

	@Override
	public void tearDown() throws InterruptedException {
		super.tearDown();
		_client.close();
	}
}
