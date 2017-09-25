package org.hong.curator.watcher;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Description: ( Curator 提供的监听子节点的PathChildrenCache 测试)
 * @author hong
 * @date 2017/9/25
 * @version v1.1
 */
public class PathChildrenCacheWatcher {

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

    /**
     * zk /nodeTest 对应子节点.
     */
    private static final String CHILD_PATH_1="/nodeTest/test-1";
    private static final String CHILD_PATH_2="/nodeTest/test-2";

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


        //4 建立一个PathChildrenCache缓存,第三个参数为是否接受节点数据内容 如果为false则不接受
        PathChildrenCache cache = new PathChildrenCache(cf, PATH, true);
        //5 在初始化的时候就进行缓存监听
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            /**
             * <B>方法名称：</B>监听子节点变更<BR>
             * <B>概要说明：</B>新建、修改、删除<BR>
             * @see org.apache.curator.framework.recipes.cache.PathChildrenCacheListener#childEvent(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent)
             */
            @Override
            public void childEvent(CuratorFramework cf, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("新增了子节点 :" + event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("修改了子节点 :" + event.getData().getPath());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("删除了子节点 :" + event.getData().getPath());
                        break;
                    default:
                        break;
                }
            }
        });

        //创建本身节点不发生变化
        cf.create().forPath(PATH, "init".getBytes());

        //添加子节点
        Thread.sleep(1000);
        cf.create().forPath(CHILD_PATH_1, "c1内容".getBytes());
        Thread.sleep(1000);
        cf.create().forPath(CHILD_PATH_2, "c2内容".getBytes());

        //修改子节点
        Thread.sleep(1000);
        cf.setData().forPath(CHILD_PATH_1, "c1更新内容".getBytes());

        //删除子节点
        Thread.sleep(1000);
        cf.delete().forPath(CHILD_PATH_2);

        //删除本身节点
        Thread.sleep(1000);
        cf.delete().deletingChildrenIfNeeded().forPath(PATH);

        Thread.sleep(Integer.MAX_VALUE);
    }
}
