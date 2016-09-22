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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayThread extends Thread {

    protected final static Logger LOG = LoggerFactory.getLogger(GatewayThread.class);

    private final int _port;
    private final int _destinationPort;
    private ServerSocket _serverSocket;
    private Lock _lock = new ReentrantLock();
    private Condition _runningCondition = _lock.newCondition();
    private boolean _running = false;

    public GatewayThread(int port, int destinationPort) {
        _port = port;
        _destinationPort = destinationPort;
        setDaemon(true);
    }

    @Override
    public void run() {
        final List<Thread> runningThreads = new Vector<Thread>();
        try {
            LOG.info("Starting gateway on port " + _port + " pointing to port " + _destinationPort);
            _serverSocket = new ServerSocket(_port);
            _lock.lock();
            try {
                _running = true;
                _runningCondition.signalAll();
            } finally {
                _lock.unlock();
            }
            while (true) {
                final Socket socket = _serverSocket.accept();
                LOG.info("new client is connected " + socket.getInetAddress());
                final InputStream incomingInputStream = socket.getInputStream();
                final OutputStream incomingOutputStream = socket.getOutputStream();

                final Socket outgoingSocket;
                try {
                    outgoingSocket = new Socket("localhost", _destinationPort);
                } catch (Exception e) {
                    LOG.warn("could not connect to " + _destinationPort);
                    continue;
                }
                final InputStream outgoingInputStream = outgoingSocket.getInputStream();
                final OutputStream outgoingOutputStream = outgoingSocket.getOutputStream();

                Thread writeThread = new Thread() {
                    @Override
                    public void run() {
                        runningThreads.add(this);
                        try {
                            int read = -1;
                            while ((read = incomingInputStream.read()) != -1) {
                                outgoingOutputStream.write(read);
                            }
                        } catch (IOException e) {
                            // ignore
                        } finally {
                            closeQuietly(outgoingOutputStream);
                            runningThreads.remove(this);
                        }
                    }

                    @Override
                    public void interrupt() {
                        try {
                            socket.close();
                            outgoingSocket.close();
                        } catch (IOException e) {
                            LOG.error("error on stopping closing sockets", e);
                        }

                        super.interrupt();
                    }
                };

                Thread readThread = new Thread() {
                    @Override
                    public void run() {
                        runningThreads.add(this);
                        try {
                            int read = -1;
                            while ((read = outgoingInputStream.read()) != -1) {
                                incomingOutputStream.write(read);
                            }
                        } catch (IOException e) {
                            // ignore
                        } finally {
                            closeQuietly(incomingOutputStream);
                            runningThreads.remove(this);
                        }
                    }
                };

                writeThread.setDaemon(true);
                readThread.setDaemon(true);

                writeThread.start();
                readThread.start();
            }
        } catch (SocketException e) {
            if (!_running) {
                throw ExceptionUtil.convertToRuntimeException(e);
            }
            LOG.info("Stopping gateway");
        } catch (Exception e) {
            LOG.error("error on gateway execution", e);
        }

        for (Thread thread : new ArrayList<Thread>(runningThreads)) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    protected void closeQuietly(Closeable closable) {
        try {
            closable.close();
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public void interrupt() {
        try {
            _serverSocket.close();
        } catch (Exception cE) {
            LOG.error("error on stopping gateway", cE);
        }
        super.interrupt();
    }

    public void interruptAndJoin() throws InterruptedException {
        interrupt();
        join();
    }

    public void awaitUp() {
        _lock.lock();
        try {
            while (!_running) {
                _runningCondition.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            _lock.unlock();
        }
    }
}
