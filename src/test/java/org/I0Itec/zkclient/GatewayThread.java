package org.I0Itec.zkclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;

public class GatewayThread extends Thread {

    protected final static Logger LOG = Logger.getLogger(GatewayThread.class);

    private final int _port;
    private final int _destinationPort;
    private ServerSocket _serverSocket;

    public GatewayThread(int port, int destinationPort) {
        _port = port;
        _destinationPort = destinationPort;
    }

    @Override
    public void run() {
        Thread readThread = null;
        Thread writeThread = null;
        try {
            LOG.info("starting gateway on port " + _port + " pointing to port " + _destinationPort);
            _serverSocket = new ServerSocket(_port);
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

                writeThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            int read = -1;
                            while ((read = incomingInputStream.read()) != -1) {
                                outgoingOutputStream.write(read);
                            }
                        } catch (IOException e) {
                            //
                        }
                        LOG.info("write thread terminated");
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

                readThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            int read = -1;
                            while ((read = outgoingInputStream.read()) != -1) {
                                incomingOutputStream.write(read);
                            }
                        } catch (IOException e) {
                            //
                        }
                        LOG.info("read thread terminated");
                    }
                };

                writeThread.setDaemon(true);
                readThread.setDaemon(true);

                writeThread.start();
                readThread.start();
            }
        } catch (SocketException e) {
            LOG.info("Stopping gateway");
        } catch (Exception e) {
            LOG.error("error on gateway execution", e);
        }
        if (writeThread != null) {
            writeThread.interrupt();
            try {
                writeThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        if (readThread != null) {
            readThread.interrupt();
            try {
                readThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
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

}
