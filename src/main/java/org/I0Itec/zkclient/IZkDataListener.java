package org.I0Itec.zkclient;

import java.io.Serializable;


/**
 * An {@link IZkDataListener} can be registered at a {@link ZkClient} for
 * listening on zk data changes for a given path.
 * 
 * Node: Also this listener re-subscribes it watch for the path on each zk event
 * (zk watches are one-timers) is is not guaranteed that events on the path are
 * missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An
 * implementation of this class should take that into account.
 */
public interface IZkDataListener {

    public void handleDataChange(String dataPath, Serializable data) throws Exception;

    public void handleDataDeleted(String dataPath) throws Exception;
}
