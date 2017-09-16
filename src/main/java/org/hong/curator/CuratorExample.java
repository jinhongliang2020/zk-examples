package org.hong.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;

/**
 * apache curator使用.
 * <p>
 * 1.Curator框架提供了一套高级的API， 简化了ZooKeeper的操作。 它增加了很多使用ZooKeeper开发的特性，可以处理ZooKeeper集群复杂的连接管理和重试机制
 * 2.简化了原生的ZooKeeper的方法，事件等
 * 3.提供了一个现代的流式接口
 */
public class CuratorExample {

    /**
     * zk 目录节点.
     */
    private static final String PATH = "/example/basic";

    /**
     * zk 连接地址.
     */
    private static final String CONNECTION_URL = "127.0.0.1:2181";

    public static void main(String[] args) {
        CuratorFramework client = null;
        try {
            // 创建连接.
            client = createZkClient(CONNECTION_URL);
            // 必须调用它的start()启动.
            client.start();

            // 判断对应节点是否存在。
            Stat stat = client.checkExists().forPath(PATH);
            if (stat == null) {
                // 创建一个目录节点.
                client.create().creatingParentsIfNeeded().forPath(PATH, "test".getBytes());
                CloseableUtils.closeQuietly(client);
            }

            // 使用Builder方式创建CuratorFramework ,获取对应节点数据.
            client = createWithOptions(CONNECTION_URL, new ExponentialBackoffRetry(1000, 3), 1000, 1000);
            client.start();
            System.out.println(new String(client.getData().forPath(PATH)));

            // 创建一个节点;
            Stat statHead = client.checkExists().forPath("/head");
            if(statHead!=null) {
                // 删除节点
                client.delete().forPath("/head");
            }
            String data = client.create().forPath("/head", "hello world".getBytes());
            System.out.println("data:..." + data);

            // 获取节点数据.
            byte[] bytes = client.getData().watched().forPath("/head");
            System.out.println("/head 节点数据:"+ new String(bytes));

            //



        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取zk 连接.
     * 使用Curator 工厂方法创建CuratorFramework 实例.
     *
     * @param connectionString zk 服务地址
     * @return
     */
    public static CuratorFramework createZkClient(String connectionString) {
        // 设置重试策略
        // 第一个参数:等待重试的时间  第二个参数：重试次数
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    /**
     * 获取zk 连接.
     * 使用Curator builder模式创建CuratorFramework实例.
     *
     * @param connectionString
     * @param retryPolicy
     * @param connectionTimeoutMs
     * @param sessionTimeoutMs
     * @return
     */
    public static CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                // 连接超时时间.
                .connectionTimeoutMs(connectionTimeoutMs)
                // 会话超时时间.
                .sessionTimeoutMs(sessionTimeoutMs)
                // etc. etc.
                .build();
    }

}
