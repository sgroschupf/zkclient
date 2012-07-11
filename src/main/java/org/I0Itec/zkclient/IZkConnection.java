/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public interface IZkConnection {

    public void connect(Watcher watcher);

    void close() throws InterruptedException;

    public void delete(String path) throws InterruptedException, KeeperException;

    public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException;

    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException;
    
    public void create(String path, byte[] data, CreateMode mode, AsyncCallback.StringCallback callback, Object context) throws KeeperException, InterruptedException;
    
    public void delete(String path, AsyncCallback.VoidCallback callback, Object context) throws InterruptedException, KeeperException;

    boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException;

    List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException;

    public void getChildren(final String path, final boolean watch, AsyncCallback.ChildrenCallback callback, Object context ) throws KeeperException, InterruptedException;

    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException;

    public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException;

    public States getZookeeperState();

    public long getCreateTime(String path) throws KeeperException, InterruptedException;

    public String getServers();
    
    public void addAuthInfo(String scheme, byte[] auth);
    
    /**
     * Support the interface for user to get the state of zookeeper.<br>
     * See more: https://github.com/sgroschupf/zkclient/issues/20<br>
     * @return if null != zookeeper,return zookeeper.getState() <br>
     *       else return States.CLOSED
     * */
    public States getState();
}