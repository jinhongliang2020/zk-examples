package org.hong.zk.watcher;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;


/**
 * 测试自己实现的Watcher
 */
public class TestZkWatcher {

    public static ZooKeeper zk = null;
    /**
     * zookeeper地址
     */
    public static final String CONNECT_URL = "127.0.0.1:2181";
    /**
     * session超时时间
     */
    public static final int SESSION_OUTTIME = 3000;//ms
    /**
     * zk数据节点
     **/
    public static final String NODE_PATH = "/nodeTest";

    public static void main(String[] args) {
        try {
            ZkWatcher zkWatcher = new ZkWatcher();
            zk = new ZooKeeper(CONNECT_URL, SESSION_OUTTIME, zkWatcher);


            // 判断节点是否存在
            Stat stat = zk.exists(NODE_PATH, true);
            if (stat == null) {
                // 创建节点
                zk.create(NODE_PATH, "111".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            stat = zk.exists(NODE_PATH, true);
            if (stat != null) {
                System.out.println("nodeTest节点存在.");
            }

            // 获取节点数据
            String data = new String(zk.getData(NODE_PATH,true,stat));
            System.out.println(NODE_PATH + "节点数据：" + data);

            //获取对应所有节点，返回一个List<String>
            List<String> nodeList = zk.getChildren(NODE_PATH,true);
            nodeList.forEach(node -> System.out.println(node));

            //修改节点的数据
            zk.setData(NODE_PATH, "222".getBytes(),-1);
            System.out.println(NODE_PATH + "节点数据：" + zk.getData(NODE_PATH,true,stat));


            //删除节点 注：zk提供的客户端是不支持递归删除的，所以需要我们自己实现
            zk.delete(NODE_PATH,-1);
            System.out.println(NODE_PATH + "是否存在：" + zk.exists(NODE_PATH,true));


        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
