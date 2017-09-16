package org.hong.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * zk 原生客户端编程测试.
 * Created by hong on 2017/9/2.
 */
public class ZkClientWatcher implements Watcher {

    private static ZooKeeper zk = null;
    public static ReentrantLock lock = new ReentrantLock();
    public static Condition condition = lock.newCondition();

    public static void main(String[] args) {
        try {
            lock.lock();//请求锁
            // 通过 种方式注 的watcher将会作 整个zk会话期间的默认watcher，会一直被 保 在客户端ZKWatchManager的defaultWatcher中，
            // 如果有 它的 置，则 个 watcher会被覆盖
            ZkClientWatcher zkClientWatcher = new ZkClientWatcher();
            zk = getZkClient("127.0.0.1:2181", 3000, zkClientWatcher);

            Thread.sleep(3000);

            // 主动watcher 一下.
            Stat stat =new Stat();
            zk.getData("/nodeTest", true,stat);

            // 判断节点是否存在
            Boolean exists = zkClientWatcher.isExists("/nodeTest");
            if(!exists){
                // 创建节点
                zkClientWatcher.createPath("/nodeTest", "111");
            }
            System.out.println("nodeTest节点是否存在：" + exists);

            // 获取节点数据
            String data = zkClientWatcher.readData("/nodeTest");
            System.out.println("nodeTest节点数据：" + data);

            //获取对应所有节点，返回一个List<String>
            List<String> nodeList = zk.getChildren("/nodeTest", true);
            nodeList.forEach(node -> System.out.println(node));

            // 保证主程序不停止，否则无法查看watchedEvent 返回内容.
            // 设置当前线程进入等待
            condition.await();
        } catch (InterruptedException e) {
            System.out.println("连接创建失败，发生 InterruptedException , e " + e.getMessage());
        } catch (KeeperException e) {
            System.out.println("获取zk数据异常, e " + e.getMessage());
        }

    }

    private static ZooKeeper getZkClient(String s, int i, ZkClientWatcher zkClientWatcher) {
        ZooKeeper zk=null;
        try {
            zk= new ZooKeeper("localhost:2181", 3000, zkClientWatcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zk;
    }

    public void process(WatchedEvent watchedEvent) {
        String path = watchedEvent.getPath();
        System.out.println("node path:"+path);

        // Watcher 置 ，一旦触发一次即会失效，如果需要一直监听 ，就需要重新注册
        // 这算是原生api 没有处理好的地方
        try {
            ZkClientWatcher watcher = new ZkClientWatcher();
            // 注：在重新监听的时候，需要注意是监听当前节点还是子节点.
            zk.exists(path, watcher);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建节点
     * <p>创建zNode节点, String create(path<节点路径>, data[]<节点内容>, List(ACL访问控制列表), CreateMode<zNode创建类型>) </p><br/>
     * <pre>
     *     ACL访问控制列表
     *     ZooDefs.Ids.OPEN_ACL_UNSAFE 表示所有人都能访问所有权限
     *
     *     节点创建类型(CreateMode)
     *     1、PERSISTENT:持久化节点
     *     2、PERSISTENT_SEQUENTIAL:顺序自动编号持久化节点，这种节点会根据当前已存在的节点数自动加 1
     *     3、EPHEMERAL:临时节点客户端,session超时这类节点就会被自动删除
     *     4、EPHEMERAL_SEQUENTIAL:临时自动编号节点
     * </pre>
     *
     * @param path zNode节点路径
     * @param data zNode数据内容
     * @return 创建成功返回true, 反之返回false.
     */
    public boolean createPath(String path, String data) {
        try {
            String zkPath = this.zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("节点创建成功, Path: " + zkPath + ", content: " + data);
            return true;
        } catch (KeeperException e) {
            System.out.println("节点创建失败, 发生KeeperException! path: " + path + ", data:" + data
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("节点创建失败, 发生 InterruptedException! path: " + path + ", data:" + data
                    + ", errMsg:" + e.getMessage());
        }
        return false;
    }


    /**
     * 删除节点
     * <p>删除一个zMode节点, void delete(path<节点路径>, stat<数据版本号>)</p><br/>
     * <pre>
     *     说明
     *     1、版本号不一致,无法进行数据删除操作.
     *     2、如果版本号与znode的版本号不一致,将无法删除,是一种乐观加锁机制;如果将版本号设置为-1,不会去检测版本,直接删除.
     * </pre>
     *
     * @param path zNode节点路径
     * @return 删除成功返回true, 反之返回false.
     */
    public boolean deletePath(String path) {
        try {
            this.zk.delete(path, -1);
            System.out.println("节点删除成功, Path: " + path);
            return true;
        } catch (KeeperException e) {
            System.out.println("节点删除失败, 发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("节点删除失败, 发生 InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage());
        }
        return false;
    }

    /**
     * 节点赋值/更新节点
     * <p>更新指定节点数据内容, Stat setData(path<节点路径>, data[]<节点内容>, stat<数据版本号>)</p>
     * <pre>
     *     设置某个znode上的数据时如果为-1，跳过版本检查
     * </pre>
     *
     * @param path zNode节点路径
     * @param data zNode数据内容
     * @return 更新成功返回true, 返回返回false
     */
    public boolean writeData(String path, String data) {
        try {
            Stat stat = this.zk.setData(path, data.getBytes(), -1);
            System.out.println("更新数据成功, path：" + path + ", stat: " + stat);
            return true;
        } catch (KeeperException e) {
            System.out.println("更新数据失败, 发生KeeperException! path: " + path + ", data:" + data
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("更新数据失败, 发生InterruptedException! path: " + path + ", data:" + data
                    + ", errMsg:" + e.getMessage());
        }
        return false;
    }


    /**
     * <p>读取指定节点数据内容,byte[] getData(path<节点路径>, watcher<监视器>, stat<数据版本号>)</p>
     *
     * @param path zNode节点路径
     * @return 节点存储的值, 有值返回, 无值返回null
     */
    public String readData(String path) {
        String data = null;
        try {
            data = new String(this.zk.getData(path, false, null));
            System.out.println("读取数据成功, path:" + path + ", content:" + data);
        } catch (KeeperException e) {
            System.out.println("读取数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("读取数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage());
        }
        return data;
    }


    /**
     * <p>判断某个zNode节点是否存在, Stat exists(path<节点路径>, watch<并设置是否监控这个目录节点，这里的 watcher 是在创建 ZooKeeper 实例时指定的 watcher>)</p>
     *
     * @param path zNode节点路径
     * @return 存在返回true, 反之返回false
     */
    public boolean isExists(String path) {
        try {
            Stat stat = this.zk.exists(path,true);
            return null != stat;
        } catch (KeeperException e) {
            System.out.println("读取数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("读取数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage());
        }
        return false;
    }


    /**
     * <p>获取某个节点下的所有子节点,List getChildren(path<节点路径>, watcher<监视器>)该方法有多个重载</p>
     *
     * @param path zNode节点路径
     * @return 子节点路径集合 说明,这里返回的值为节点名
     * <pre>
     *     eg.
     *     /node
     *     /node/child1
     *     /node/child2
     *     getChild( "node" )户的集合中的值为["child1","child2"]
     * </pre>
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChild(String path) {
        try {
            List<String> list = this.zk.getChildren(path, false);
            if (list.isEmpty()) {
                System.out.println("中没有节点" + path);
            }
            return list;
        } catch (KeeperException e) {
            System.out.println("读取子节点数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("读取子节点数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage());
        }
        return null;
    }

}
