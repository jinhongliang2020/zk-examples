package org.hong.curator.barrier;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Description: ( Curator 实现分布式栅栏.)
 * @author hong
 * @date 2017/9/25
 * @version v1.1
 * @see DistributedBarrier
 */
public class ZkDistributedBarrier {
    /**
     * zk 连接地址.
     */
    private static final String CONNECTION_URL = "127.0.0.1:2181";

    /**
     * zk 目录节点.
     */
    private static final String PATH = "/lock";

    /**
     * 会话超时时间.
     */
    private static final int SESSION_OUTTIME =3000;
    private final static CountDownLatch down = new CountDownLatch(1);
    private static DistributedBarrier barrier;
    private static final int thread = 5;
    public static void main(String[] args) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(thread);
        for (int i = 0; i < thread; i++) {
            final int index = i;
            service.submit(new Runnable() {
                public void run() {
                    try {
                        new ZkDistributedBarrier().schedule(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        down.countDown();

        Thread.sleep(2000);
        //移除栏栅
        barrier.removeBarrier();

        Thread.sleep(2000);
        service.shutdownNow();
    }

    private void schedule(int index) throws Exception {
        down.await();
        CuratorFramework client = this.getStartedClient(index);
        barrier = new DistributedBarrier(client, PATH);
        System.out.println("Thread [" + index + "] 开始准备!");
        barrier.setBarrier();
        barrier.waitOnBarrier();
        System.out.println("Thread [" + index + "] 开始运行!");
    }


    private CuratorFramework getStartedClient(final int index) {
        RetryPolicy rp = new ExponentialBackoffRetry(SESSION_OUTTIME, 3);
        // Fluent风格创建
        CuratorFramework cfFluent = CuratorFrameworkFactory.builder().connectString("localhost:2181")
                .sessionTimeoutMs(SESSION_OUTTIME).connectionTimeoutMs(SESSION_OUTTIME).retryPolicy(rp).build();
        cfFluent.start();
        // System.out.println("Thread [" + index + "] Server connected...");
        return cfFluent;
    }
}
