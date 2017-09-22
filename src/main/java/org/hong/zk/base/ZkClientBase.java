package org.hong.zk.base;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * zk 原生客户端编程测试.
 * Created by hong on 2017/9/2.
 */
public class ZkClientBase {

    public static ZooKeeper zk = null;
    /**
     * zookeeper地址
     */
    public static final String CONNECT_URL = "192.168.0.90:2181";
    /**
     * session超时时间
     */
    public static final int SESSION_OUTTIME = 3000;//ms
    /**
     * zk数据节点
     **/
    public static final String NODE_PATH = "/nodeTest";
    /**
     * 递归测试节点
     **/
    public static final String NODE_CHILD_PATH = "/nodeTest2/test";

    public static void main(String[] args) {
        try {
            zk = getZkClient(CONNECT_URL, SESSION_OUTTIME);

            // 判断节点是否存在
            Boolean exists = isExists(NODE_PATH);
            if (!exists) {
                // 创建节点
                createPath(NODE_PATH, "111");
            }
            exists = isExists(NODE_PATH);
            System.out.println("nodeTest节点是否存在：" + exists);

            // 获取节点数据
            String data = readData(NODE_PATH);
            System.out.println(NODE_PATH+"节点数据："+ data);

            //获取对应所有节点，返回一个List<String>
            List<String> nodeList = getChild(NODE_PATH);
            nodeList.forEach(node -> System.out.println(node));

            //修改节点的数据
            writeData(NODE_PATH,"222");
            System.out.println(NODE_PATH+"节点数据："+readData(NODE_PATH));


            //删除节点 注：zk提供的客户端是不支持递归删除的，所以需要我们自己实现
            deletePath(NODE_PATH);
            System.out.println(NODE_PATH+"是否存在："+isExists(NODE_PATH));


            //创建子节点
           createChildPath(NODE_CHILD_PATH,new String[]{"111","222"});

            //删除多级节点 有问题
            deleteChildPath(NODE_CHILD_PATH);

        } catch (Exception e) {
            System.out.println("连接创建失败，发生 InterruptedException , e " + e.getMessage());
        }
    }


    /**
     * 获取zk连接.
     *
     * @param connectUrl
     * @param outtime
     * @return
     */
    private static ZooKeeper getZkClient(String connectUrl, int outtime) {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(connectUrl, outtime, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("当前执行的操作类型： " + watchedEvent.getType());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zk;
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
    public static boolean createPath(String path, String data) {
        try {
            String zkPath = zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
     * 创建多级节点.
     *
     * @param nodePath  节点路径
     * @param nodeDatas 对应多节点数据
     * @return
     */
    public static boolean createChildPath(String nodePath, String[] nodeDatas) {
        String[] nodes = nodePath.split("/");
        int nodeCount = nodes.length;
        if (nodeCount > 2) {
            int count = 1;
            StringBuffer buffer = new StringBuffer();
            while (count < nodeCount) {
                buffer.append("/");
                buffer.append(nodes[count]);
                if (!isExists(buffer.toString()))
                    createPath(buffer.toString(), nodeDatas[count - 1]);
                count++;
            }
            System.out.println("多级节点创建成功..." + nodePath);
            return true;
        } else {
            System.out.println("对不起,不是多级节点...");
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
    public static boolean deletePath(String path) {
        try {

            zk.delete(path, -1);
            System.out.println("节点删除成功, Path: " + path);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
            System.out.println("节点删除失败, 发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("节点删除失败, 发生 InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage());
        }
        return false;
    }

    /**
     * 递归删除多级节点
     *
     * @param path
     * @return
     */
    public static boolean deleteChildPath(String path) {
        try {
            System.out.println(readData(path));
            while (notEntity(path) && !isExists(path)) {
                deletePath(path);
                path = path.substring(0, path.lastIndexOf("/"));
            }
            System.out.println("多级节点删除成功...");
        } catch (Exception e) {
            System.out.println("节点删除失败, path: " + path + ", errMsg:" + e.getMessage());
        }
        return true;
    }

    private static boolean notEntity(String str) {
        return str != null && !"".equals(str);
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
    public static boolean writeData(String path, String data) {
        try {
            Stat stat = zk.setData(path, data.getBytes(), -1);
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
    public static String readData(String path) {
        String data = null;
        try {
            data = new String(zk.getData(path, false, null));
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
    public static boolean isExists(String path) {
        try {
            Stat stat = zk.exists(path, true);
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
    public static List<String> getChild(String path) {
        try {
            List<String> list = zk.getChildren(path, false);
            if (list.isEmpty()) {
                System.out.println(path + "中没有子节点" + path);
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
