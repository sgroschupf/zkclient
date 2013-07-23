/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All listeners registered at the {@link ZkClient} will be notified from this event thread. This is to prevent
 * dead-lock situations. The {@link ZkClient} pulls some information out of the {@link ZooKeeper} events to signal
 * {@link ZkLock} conditions. Re-using the {@link ZooKeeper} event thread to also notify {@link ZkClient} listeners,
 * would stop the ZkClient from receiving events from {@link ZooKeeper} as soon as one of the listeners blocks (because
 * it is waiting for something). {@link ZkClient} would then for instance not be able to maintain it's connection state
 * anymore.
 */
class ZkEventThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ZkEventThread.class);

    private BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue<ZkEvent>();

    private static AtomicInteger _eventId = new AtomicInteger(0);

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

    ZkEventThread(String name) {
        setDaemon(true);
        setName("ZkClient-EventThread-" + getId() + "-" + name);
    }

    @Override
    public void run() {
        LOG.info("Starting ZkClient event thread.");
        try {
            while (!isInterrupted()) {
                ZkEvent zkEvent = _events.take();
                int eventId = _eventId.incrementAndGet();
                LOG.debug("Delivering event #" + eventId + " " + zkEvent);
                try {
                    zkEvent.run();
                } catch (InterruptedException e) {
                    interrupt();
                } catch (ZkInterruptedException e) {
                    interrupt();
                } catch (Throwable e) {
                    LOG.error("Error handling event " + zkEvent, e);
                }
                LOG.debug("Delivering event #" + eventId + " done");
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
