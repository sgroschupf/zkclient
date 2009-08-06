package org.I0Itec.zkclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class BaseZkClientTest {

	private static final Logger LOG = Logger.getLogger(BaseZkClientTest.class);
	protected ZkServer _zkServer;
	protected ZkClient _client;

	@Before
	public void setUp() throws InterruptedException, IOException,
			KeeperException {
		LOG.info("------------ BEFORE -------------");

	}

	@After
	public void tearDown() throws InterruptedException {
		LOG.info("------------ AFTER -------------");

	}

	@Test(expected = IOException.class, timeout = 5000)
	public void testUnableToConnect() throws Exception {
		LOG.info("--- testUnableToConnect");
		// trying to a non existing zk server
		new ZkClient("localhost:4799", 1000);
	}

	@Test
	public void testWriteAndRead() throws Exception {
		LOG.info("--- testWriteAndRead");

		String data = "something";
		String path = "/a";
		_client.createPersistent(path, data);
		String data2 = _client.readData(path);
		Assert.assertEquals(data, data2);
		_client.delete(path);

	}

	@Test
	public void testDelete() throws Exception {
		LOG.info("--- testDelete");
		String path = "/a";
		assertFalse(_client.delete(path));
		_client.createPersistent(path, null);
		assertTrue(_client.delete(path));
		assertFalse(_client.delete(path));
	}

	@Test
	public void testDeleteRecursive() throws Exception {
		LOG.info("--- testDeleteRecursive");
		// should be able to call this on a not existing directory
		_client.deleteRecursive("/doesNotExist");
	}

	@Test(timeout = 15000)
	public void testRetryUntilConnected() throws Exception {
		LOG.info("--- testRetryUntilConnected");
		final ZkConnection connection = new ZkConnection("localhost:4711");
		final ZkClient _client = new ZkClient(connection);
		_zkServer.shutdown();
		_zkServer.join();

		// start server in 250ms
		new DeferredZookeeperStarter(_zkServer, 250).start();

		// this should work as soon as the connection is reestablished, if it
		// fails it throws a ConnectionLossException
		_client.retryUntilConnected(new Callable<Object>() {

			@Override
			public Object call() throws Exception {
				connection.exists("/a", false);
				return null;
			}
		});
	}

	@Test(timeout = 15000)
	public void testWaitUntilConnected() throws Exception {
		LOG.info("--- testWaitUntilConnected");
		ZkClient _client = new ZkClient("localhost:4711", 5000);

		_zkServer.shutdown();
		_zkServer.join();

		// the _client state should change to KeeperState.Disconnected
		assertTrue(_client.waitForKeeperState(KeeperState.Disconnected, 1,
				TimeUnit.SECONDS));

		// connection should not be possible and timeout after 100ms
		assertFalse(_client.waitUntilConnected(100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testWaitUntilExists() throws InterruptedException, IOException,
			KeeperException {
		LOG.info("--- testWaitUntilExists");

		// create /gaga node asynchronously
		new Thread() {
			@Override
			public void run() {
				try {
					Thread.sleep(100);
					_client.createPersistent("/gaga");
				} catch (Exception e) {
					// ignore
				}
			}
		}.start();

		// wait until this was created
		assertTrue(_client.waitUntilExists("/gaga", TimeUnit.SECONDS, 5));
		assertTrue(_client.exists("/gaga"));

		// waiting for /neverCreated should timeout
		assertFalse(_client.waitUntilExists("/neverCreated",
				TimeUnit.MILLISECONDS, 100));
	}
}
