package org.I0Itec.zkclient.serialize;

import org.I0Itec.zkclient.exception.ZkMarshallingError;

/**
 * A {@link ZkSerializer} which simply passes byte arrays to zk and back.
 */
public class BytesPushThroughSerializer implements ZkSerializer {

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return bytes;
    }

    @Override
    public byte[] serialize(Object bytes) throws ZkMarshallingError {
        return (byte[]) bytes;
    }

}
