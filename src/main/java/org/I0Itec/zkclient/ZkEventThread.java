package org.I0Itec.zkclient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * All listeners registered at the {@link ZkClient} will be notified from this event thread. This is not prevent dead-lock situations. The ZkClient pulls some
 * information out of the Zookeeper events and signals {@link ZkLock} conditions. Using the same Zookeeper event thread to also notify listeners, would stop the
 * ZkClient from maintaining those properties as soon as one of the listeners doesn't return. This can for example happen, if one of the listeners waits until
 * ZkClient changes its status to SyncConnected.
 */
class ZkEventThread extends Thread {

    private static final Logger LOG = Logger.getLogger(ZkEventThread.class);

    private BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue<ZkEvent>();

    static abstract class ZkEvent {

        private String _description;

        public ZkEvent(String description) {
            _description = description;
        }

        public abstract void run() throws Exception;

        @Override
        public String toString() {
            return "ZkEvent[" + _description + "]";
        }
    }

    ZkEventThread() {
        setDaemon(true);
        setName("ZkClient-EventThread");
    }

    @Override
    public void run() {
        LOG.info("Starting ZkClient event thread.");
        try {
            while (!isInterrupted()) {
                ZkEvent zkEvent = _events.take();
                LOG.debug("Delivering event " + zkEvent);
                try {
                    zkEvent.run();
                } catch (InterruptedException e) {
                    interrupt();
                } catch (Exception e) {
                    LOG.warn("Error handling event " + zkEvent, e);
                }
                LOG.debug("Delivering event done");
            }
        } catch (InterruptedException e) {
            LOG.info("Terminate ZkClient event thread.");
        }
    }

    public void send(ZkEvent event) {
        if (!isInterrupted()) {
            LOG.debug("New event: " + event);
            _events.add(event);
        }
    }
}
