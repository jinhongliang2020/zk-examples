package org.hong.curator.lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;


/**
 * @Description: ( 使用Curator 提供的InterProcessMutex 实现分布式锁)
 * @author hong
 * @date 2017/9/25
 * @version v1.1
 */
public class ZkInterProcessMutex {

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

    static int count = 10;
    public static void genarNo(){
        try {
            count--;
            System.out.println( Thread.currentThread().getName() +": "+count);
        } finally {

        }
    }

    public static void main(String[] args) throws InterruptedException {

        //1 重试策略：初试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //2 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECTION_URL)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();
        //3 开启连接
        cf.start();

        //4 分布式锁
        final InterProcessMutex lock = new InterProcessMutex(cf, PATH);
        final CountDownLatch countdown = new CountDownLatch(1);

        for(int i = 0; i < 10; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countdown.await();
                        //加锁
                        lock.acquire();
                        //-------------业务处理开始
                        genarNo();
                        //-------------业务处理结束
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            //释放
                            lock.release();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            },"t" + i).start();
        }
        Thread.sleep(100);
        countdown.countDown();
    }

}
