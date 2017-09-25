package org.hong.curator.watcher;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;


/**
 * @Description: ( Curator 提供的监听当前节点创建节点和更新节点的NodeCache测试 )
 * @author hong
 * @date 2017/9/25
 * @version v1.1
 */
public class NodeCacheWatcher {

    /**
     * zk 连接地址.
     */
    private static final String CONNECTION_URL = "127.0.0.1:2181";

    /**
     * 会话超时时间.
     */
    private static final int SESSION_OUTTIME =3000;

    /**
     * zk 目录节点.
     */
    private static final String PATH="/nodeTest";

    public static void main(String[] args) throws Exception {

        //1 重试策略：初试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //2 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECTION_URL)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();

        //3 建立连接
        cf.start();

        //4 建立一个cache缓存
        final NodeCache cache = new NodeCache(cf, PATH, false);
        cache.start(true);
        cache.getListenable().addListener(new NodeCacheListener() {
            /**
             * <B>方法名称：</B>nodeChanged<BR>
             * <B>概要说明：</B>触发事件为创建节点和更新节点，在删除节点的时候并不触发此操作。<BR>
             * @see org.apache.curator.framework.recipes.cache.NodeCacheListener#nodeChanged()
             */
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("路径为：" + cache.getCurrentData().getPath());
                System.out.println("数据为：" + new String(cache.getCurrentData().getData()));
                System.out.println("状态为：" + cache.getCurrentData().getStat());
                System.out.println("---------------------------------------");
            }
        });

        Thread.sleep(1000);
        cf.create().forPath(PATH, "123".getBytes());

        Thread.sleep(1000);
        cf.setData().forPath(PATH, "456".getBytes());

        Thread.sleep(1000);
        cf.delete().forPath(PATH);

        Thread.sleep(Integer.MAX_VALUE);
    }
}
