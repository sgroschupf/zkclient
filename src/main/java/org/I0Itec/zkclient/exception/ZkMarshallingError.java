package org.I0Itec.zkclient.exception;



public class ZkMarshallingError extends ZkException {

    private static final long serialVersionUID = 1L;

    public ZkMarshallingError() {
        super();
    }

    public ZkMarshallingError(Throwable cause) {
        super(cause);
    }

    public ZkMarshallingError(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkMarshallingError(String message) {
        super(message);
    }
}
