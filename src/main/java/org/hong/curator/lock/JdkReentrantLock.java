package org.hong.curator.lock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description: (单点环境中ReenTranLock锁,来保证线程安全.)
 * @author hong
 * @date 2017/9/25
 * @version v1.1
 */
public class JdkReentrantLock {

    /**
     * java 显式锁ReentrantLock
     */
    static ReentrantLock reentrantLock = new ReentrantLock();
    static int count = 10;
    public static void genarNo(){
        try {
            reentrantLock.lock();
            count--;
            System.out.println( Thread.currentThread().getName() +": "+count);
        } finally {
            reentrantLock.unlock();
        }
    }

    public static void main(String[] args) throws Exception{

        final CountDownLatch countdown = new CountDownLatch(1);
        for(int i = 0; i < 10; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countdown.await();
                        genarNo();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                    }
                }
            },"t" + i).start();
        }
        Thread.sleep(50);
        countdown.countDown();


    }
}
