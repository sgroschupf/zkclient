package org.I0Itec.zkclient;

public class Holder<T> {

    private T _value;

    public Holder() {
        // do nothing
    }

    public Holder(T value) {
        _value = value;
    }

    public T get() {
        return _value;
    }

    public void set(T value) {
        _value = value;
    }
}
