package org.hong.curator.atomicinteger;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

/**
 * @Description: ( 使用Curator 实现分布式计数器.)
 * @author hong
 * @date 2017/9/25
 * @version v1.1
 */
public class CuratorAtomicInteger {

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

    public static void main(String[] args) throws Exception {

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


        //4 使用DistributedAtomicInteger
        DistributedAtomicInteger atomicIntger =
                new DistributedAtomicInteger(cf, "/super", new RetryNTimes(3, 1000));

        AtomicValue<Integer> value = atomicIntger.add(1);
        System.out.println(value.succeeded());
        System.out.println(value.postValue());	//最新值
        System.out.println(value.preValue());	//原始值
    }

}
