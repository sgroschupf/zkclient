package org.I0Itec.zkclient;

import org.apache.zookeeper.Watcher.Event.KeeperState;

public interface IZkStateListener {

    /**
     * Called when the zookeeper connection state has changed.
     * 
     * @param state
     *            The new state.
     * @throws Exception
     *             On any error.
     */
    public void handleStateChanged(KeeperState state) throws Exception;

    /**
     * Called after the zookeeper session has expired and a new session has been
     * created. You would have to re-create any ephemeral nodes here.
     * 
     * @throws Exception On any error.
     */
    public void handleNewSession() throws Exception;
}
