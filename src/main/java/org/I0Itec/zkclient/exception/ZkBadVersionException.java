package org.I0Itec.zkclient.exception;

import org.apache.zookeeper.KeeperException;

public class ZkBadVersionException extends ZkException {

    private static final long serialVersionUID = 1L;

    public ZkBadVersionException() {
        super();
    }

    public ZkBadVersionException(KeeperException cause) {
        super(cause);
    }

    public ZkBadVersionException(String message, KeeperException cause) {
        super(message, cause);
    }

    public ZkBadVersionException(String message) {
        super(message);
    }
}
