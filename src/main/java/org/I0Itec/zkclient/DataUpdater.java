package org.I0Itec.zkclient;


/**
 * Updates the data of a znode. This is used together with {@link ZkClient#updateDataSerialized(String, DataUpdater)}.
 * 
 * @param <T>
 */
public interface DataUpdater<T extends Object> {

    /**
     * Updates the current data of a znode.
     * 
     * @param currentData
     *            The current contents.
     * @return the new data that should be written back to ZooKeeper.
     */
    public T update(T currentData);

}
