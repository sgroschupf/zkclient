package org.I0Itec.zkclient.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.I0Itec.zkclient.exception.ZkMarshallingError;

public class SerializableSerializer implements ZkSerializer {

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Object object = inputStream.readObject();
            return object;
        } catch (ClassNotFoundException e) {
            throw new ZkMarshallingError("Unable to find object class.", e);
        } catch (IOException e) {
            throw new ZkMarshallingError(e);
        }
    }

    @Override
    public byte[] serialize(Object serializable) throws ZkMarshallingError {
        try {
            ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
            ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
            stream.writeObject(serializable);
            stream.close();
            return byteArrayOS.toByteArray();
        } catch (IOException e) {
            throw new ZkMarshallingError(e);
        }
    }

}
