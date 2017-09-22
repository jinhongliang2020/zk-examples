package org.hong.zk.auth;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZkAuthWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (event == null) {
            return;
        }
        // 连接状态
        Event.KeeperState keeperState = event.getState();
        // 事件类型
        Event.EventType eventType = event.getType();
        // 受影响的path
        String path = event.getPath();

        String logPrefix = "【Watcher】";
        System.out.println(logPrefix + "收到Watcher通知,受影响的path: " + path);
        System.out.println(logPrefix + "连接状态:\t" + keeperState.toString());
        System.out.println(logPrefix + "事件类型:\t" + eventType.toString());
        if (Event.KeeperState.SyncConnected == keeperState) {
            // 成功连接上ZK服务器
            if (Event.EventType.None == eventType) {
                System.out.println(logPrefix + "成功连接上ZK服务器");
            }
        } else if (Event.KeeperState.Disconnected == keeperState) {
            System.out.println(logPrefix + "与ZK服务器断开连接");
        } else if (Event.KeeperState.AuthFailed == keeperState) {
            System.out.println(logPrefix + "权限检查失败");
        } else if (Event.KeeperState.Expired == keeperState) {
            System.out.println(logPrefix + "会话失效");
        }
        System.out.println("--------------------------------------------");
    }
}
