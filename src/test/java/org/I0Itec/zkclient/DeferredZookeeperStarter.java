package org.I0Itec.zkclient;

class DeferredZookeeperStarter extends Thread {

    private final ZkServer _zkServer;
    private int _delay;

    DeferredZookeeperStarter(ZkServer zkServer, int delay) {
        _zkServer = zkServer;
        _delay = delay;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(_delay);
            _zkServer.start();
        } catch (Exception e) {
            // ignore
        }
    }
}