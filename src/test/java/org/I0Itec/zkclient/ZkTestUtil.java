package org.I0Itec.zkclient;

import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;

public class ZkTestUtil {

    public static ZkServer startZkServer(String testName, int port) throws InterruptedException, IOException, KeeperException {
        String dataPath = "./build/test/" + testName + "/data";
        String logPath = "./build/test/" + testName + "/log";
        FileUtils.deleteDirectory(new File(dataPath));
        FileUtils.deleteDirectory(new File(logPath));
        ZkServer zkServer = new ZkServer(dataPath, logPath, mock(IDefaultNameSpace.class), port);
        zkServer.start();
        return zkServer;
    }
}
