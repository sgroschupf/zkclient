package org.I0Itec.zkclient.exception;

import org.apache.zookeeper.KeeperException;

public class ZkException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ZkException() {
        super();
    }

    public ZkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkException(String message) {
        super(message);
    }

    public ZkException(Throwable cause) {
        super(cause);
    }

    public static ZkException create(KeeperException e) {
        switch (e.code()) {
        // case DATAINCONSISTENCY:
        // return new DataInconsistencyException();
        // case CONNECTIONLOSS:
        // return new ConnectionLossException();
        case NONODE:
            return new ZkNoNodeException(e);
            // case NOAUTH:
            // return new ZkNoAuthException();
        case BADVERSION:
            return new ZkBadVersionException(e);
            // case NOCHILDRENFOREPHEMERALS:
            // return new NoChildrenForEphemeralsException();
        case NODEEXISTS:
            return new ZkNodeExistsException(e);
            // case INVALIDACL:
            // return new ZkInvalidACLException();
            // case AUTHFAILED:
            // return new AuthFailedException();
            // case NOTEMPTY:
            // return new NotEmptyException();
            // case SESSIONEXPIRED:
            // return new SessionExpiredException();
            // case INVALIDCALLBACK:
            // return new InvalidCallbackException();

        default:
            return new ZkException(e);
        }
    }
}
