package org.I0Itec.zkclient;

import java.util.List;

/**
 * An {@link IZkChildListener} can be registered at a {@link ZkClient} for listening on zk child changes for a given path.
 * 
 * Node: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not guaranteed that events on the path are
 * missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An implementation of this class should take that into account.
 * 
 */
public interface IZkChildListener {

    /**
     * Called when the children of the given path changed.
     * 
     * @param parentPath
     *            The parent path
     * @param currentChilds
     *            The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception;
}
