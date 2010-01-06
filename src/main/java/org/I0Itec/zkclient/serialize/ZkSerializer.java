package org.I0Itec.zkclient.serialize;

import org.I0Itec.zkclient.exception.ZkMarshallingError;

/**
 * Zookeeper is able to store data in form of byte arrays. This interfacte is a bridge between those byte-array format
 * and higher level objects.
 * 
 * @see BytesPushThroughSerializer
 * @see SerializableSerializer
 */
public interface ZkSerializer {

    public byte[] serialize(Object data) throws ZkMarshallingError;

    public Object deserialize(byte[] bytes) throws ZkMarshallingError;
}
