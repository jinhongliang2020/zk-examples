package org.hong.zkclient;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.hong.serializer.MyZkSerializer;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 第三方客户端zkclient 使用.
 * Created by hong on 2017/9/7.
 */
public class zkClientTest {
    public static ReentrantLock lock = new ReentrantLock();
    public static Condition condition = lock.newCondition();

    public static void main(String[] args) throws InterruptedException {
        lock.lock();

        //1. 创建zkClient 连接.
        // 如果是集群,以逗号分隔
        String zkServer = "127.0.0.1:2181";
        // new MyZkSerializer() 创建序列化器接口，用来序列化和反序列化
        ZkClient zkClient = new ZkClient(zkServer, 1000, 1000, new MyZkSerializer());


        //2. 获取对应zk 节点数据.
        Stat stat = new Stat();
        Object data = zkClient.readData("/zkTest", stat);
        System.out.println("获取到/zkTest 节点数据：" + data);


        //3.创建一个节点
        String nodePath = "/test";
        String zkData = "hello zk";
        createNode(zkClient, nodePath, zkData);
        System.out.println("获取到刚刚新增节点数据：" + zkClient.readData(nodePath));

        //4.删除节点
        nodePath = "/wp";
        deleteNode(zkClient, nodePath, true);

        //5.更新节点的数据
        nodePath = "/test";
        updateNodeData(zkClient, nodePath, "111");


        zkClient.subscribeChildChanges(nodePath,new ZkChildListener());
        zkClient.subscribeDataChanges(nodePath,new ZkDataListener());

        condition.await();
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
    private static void createNode(ZkClient zkClient, String nodePath, Object data) {
        if (existsNode(zkClient, nodePath)) {
            System.out.println("已存在对应节点.");
            return;
        }
        zkClient.create(nodePath, data, CreateMode.PERSISTENT);
    }


    /**
     * 删除节点.
     *
     * @param zkClient
     * @param nodePath
     * @param isDelChild 是否含有子节点 分速度
     */
    private static void deleteNode(ZkClient zkClient, String nodePath, Boolean isDelChild) {
        Boolean flag = false;
        if (!isDelChild) {
            flag = zkClient.delete(nodePath);
        } else {
            flag = zkClient.deleteRecursive(nodePath);
        }
        System.out.println("删除是否成功:" + flag);
    }





}
