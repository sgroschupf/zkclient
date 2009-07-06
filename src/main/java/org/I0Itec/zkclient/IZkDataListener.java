package org.I0Itec.zkclient;

/**
 * An {@link IZkDataListener} can be registered at a {@link ZkClient} for
 * listening on zk data changes for a given path.
 * 
 * Node: Also this listener re-subscribes it watch for the path on each zk event
 * (zk watches are one-timers) is is not guaranteed that events on the path are
 * missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An
 * implementation of this class should take that into account.
 */
import java.io.Serializable;

public interface IZkDataListener<T extends Serializable> {

    void handleDataChange(String dataPath, T data);

    void handleDataAdded(String dataPath, T data);

    void handleDataDeleted(String dataPath);
}
