package org.I0Itec.zkclient;

public class Gateway {

    private GatewayThread _thread;
    private final int _port;
    private final int _destinationPort;

    public Gateway(int port, int destinationPort) {
        _port = port;
        _destinationPort = destinationPort;
    }

    public synchronized void start() {
        if (_thread != null) {
            throw new IllegalStateException("Gateway already running");
        }
        _thread = new GatewayThread(_port, _destinationPort);
        _thread.start();
        _thread.awaitUp();
    }

    public synchronized void stop() {
        if (_thread != null) {
            try {
                _thread.interruptAndJoin();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            _thread = null;
        }
    }
}
