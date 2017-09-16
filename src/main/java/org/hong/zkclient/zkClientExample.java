package org.hong.zkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.hong.serializer.MyZkSerializer;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 第三方客户端zkclient 使用.
 * Created by hong on 2017/9/7.
 */
public class zkClientExample {

    public static ReentrantLock lock = new ReentrantLock();
    public static Condition condition = lock.newCondition();
    public static void main(String[] args) throws InterruptedException {
        lock.lock();

        //1. 创建zkClient 连接.
        // 如果是集群,以逗号分隔
        String zkServer = "127.0.0.1:2181";
        // new MyZkSerializer() 创建序列化器接口，用来序列化和反序列化
        ZkClient zkClient = getZkClient(zkServer,new MyZkSerializer());

        //2. 获取对应zk 节点数据.
        Stat stat = new Stat();
        Object data = zkClient.readData("/zkTest", stat);
        System.out.println("获取到/zkTest 节点数据：" + data);

        //3.创建一个节点
        String nodePath = "/test";
        String zkData = "hello zk";
        createNode(zkClient, nodePath, zkData);
        System.out.println("获取到刚刚新增节点数据：" + zkClient.readData(nodePath));

        /**
         * 创建节点，存储对象
         */
        ZkClient zkClientObj = getZkClient(zkServer,new SerializableSerializer());
        User user = new User();
        user.setId(1);
        user.setName("zk");
        createNode(zkClientObj,"/user-node",user);
        // 获取节点数据,返回存储User对象.
        System.out.println(zkClientObj.readData("/user-node",stat).toString());

        //4.删除节点
        nodePath = "/wp";
        deleteNode(zkClient, nodePath, true);

        //5.更新节点的数据
        nodePath = "/test";
        updateNodeData(zkClient, nodePath, "111");

        // 监听节点和数据变化.
        zkClient.subscribeChildChanges(nodePath,new ZkChildListener());
        zkClient.subscribeDataChanges(nodePath,new ZkDataListener());

        condition.await();
    }

    /**
     * 使用指定序列化方式交互数据.
     * @param zkServer
     * @return
     */
    private static ZkClient getZkClient(String zkServer, ZkSerializer zkSerializer) {
        return new ZkClient(zkServer, 1000, 1000, zkSerializer);
    }


    /**
     * 更新数据.
     * @param zkClient
     * @param nodePath
     * @param data
     */
    private static void updateNodeData(ZkClient zkClient, String nodePath, Object data) {
          if(!existsNode(zkClient,nodePath)){
              System.out.println("没有对应节点.");
              return;
          }
          zkClient.writeData(nodePath,data);
    }

    /**
     * 判断对应节点是否存在.
     *
     * @param zkClient
     * @param nodePath
     */
    private static boolean existsNode(ZkClient zkClient, String nodePath) {
        Boolean flag = zkClient.exists(nodePath);
        System.out.println("是否存在zkTest目录：" + flag);
        return flag;
    }

    /**
     * 创建一个节点.
     * <p>
     * PERSISTENT (0, false, false) 持久节点：节点创建后，会一直存在，不会因客户端会话失效而删除；
     * PERSISTENT_SEQUENTIAL (2, false, true) 持久顺序节点：基本特性与持久节点一致，创建节点的过程中，zookeeper会在其名字后自动追加一个单调增长的数字后缀，作为新的节点名
     * EPHEMERAL (1, true, false) 临时节点  客户端会话失效或连接关闭后，该节点会被自动删除，且不能再临时节点下面创建子节点，否则报如下错
     * org.apache.zookeeper.KeeperException$NoChildrenForEphemeralsException: KeeperErrorCode = NoChildrenForEphemerals for /node/child）
     * EPHEMERAL_SEQUENTIAL (3, true, true) 临时顺序节点：基本特性与临时节点一致，创建节点的过程中，zookeeper会在其名字后自动追加一个单调增长的数字后缀，作为新的节点名；
     *
     * @param zkClient
     * @param nodePath
     */
    private static String createNode(ZkClient zkClient, String nodePath, Object data) {
        if (existsNode(zkClient, nodePath)) {
            System.out.println("已存在对应节点.");
            return null;
        }
        return zkClient.create(nodePath, data, CreateMode.PERSISTENT);
    }


    /**
     * 删除节点.
     *
     * @param zkClient
     * @param nodePath
     * @param isDelChild 是否含有子节点需要删除.
     */
    private static void deleteNode(ZkClient zkClient, String nodePath, Boolean isDelChild) {
        Boolean flag;
        if (!isDelChild) {
            flag = zkClient.delete(nodePath);
        } else {
            flag = zkClient.deleteRecursive(nodePath);
        }
        System.out.println("删除是否成功:" + flag);
    }

}
