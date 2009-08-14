package org.I0Itec.zkclient;


public interface IDefaultNameSpace {

    /**
     * Creates a set of default folder structure within a zookeeper .
     * 
     * @param zkClient
     *            The zkclient.
     */
    public void createDefaultNameSpace(ZkClient zkClient);
}
