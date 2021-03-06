package org.hong.zk.auth;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

/**
 * zk auth 认证测试.
 */
public class ZkAuth{

    /** 连接地址 **/
    final static String CONNECT_ADDR = "127.0.0.1:2181";
    /** 测试路径 **/
    final static String PATH = "/testAuth";
    final static String PATH_DEL = "/testAuth/delNode";
    /** 认证类型 **/
    final static String authentication_type = "digest";
    /** 认证正确方法 **/
    final static String correctAuthentication = "123456";
    /** 认证错误方法 **/
    final static String badAuthentication = "654321";

    static ZooKeeper zk = null;
    /** 标识 **/
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";

    public static void main(String[] args) throws Exception {
        ZkAuth testAuth = new ZkAuth();
        ZkAuthWatcher zkAuthWatcher =new ZkAuthWatcher();
        testAuth.createConnection(CONNECT_ADDR, 2000,zkAuthWatcher);
        List<ACL> acls = new ArrayList<ACL>(1);
        for (ACL ids_acl : ZooDefs.Ids.CREATOR_ALL_ACL) {
            acls.add(ids_acl);
        }

        try {
            zk.create(PATH, "init content".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key：" + correctAuthentication + "创建节点：" + PATH + ", 初始内容是: init content");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            zk.create(PATH_DEL, "will be deleted! ".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key：" + correctAuthentication + "创建节点：" + PATH_DEL + ", 初始内容是: init content");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 获取数据
        getDataByNoAuthentication();
        getDataByBadAuthentication();
        getDataByCorrectAuthentication();

        // 更新数据
        updateDataByNoAuthentication();
        updateDataByBadAuthentication();
        updateDataByCorrectAuthentication();

        // 删除数据
        deleteNodeByBadAuthentication();
        deleteNodeByNoAuthentication();
        deleteNodeByCorrectAuthentication();

        Thread.sleep(1000);

        deleteParent();
        //释放连接
        testAuth.releaseConnection();
    }

    /**
     * 创建ZK连接
     *
     * @param connectString  ZK服务器地址列表
     * @param sessionTimeout Session超时时间
     */
    public void createConnection(String connectString, int sessionTimeout,Watcher watcher) {
        this.releaseConnection();
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, watcher);
            //添加节点授权
            zk.addAuthInfo(authentication_type, correctAuthentication.getBytes());
            System.out.println(LOG_PREFIX_OF_MAIN + "开始连接ZK服务器");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * 获取数据：采用错误的密码
     */
    static void getDataByBadAuthentication() {
        String prefix = "[使用错误的授权信息]";
        try {
            ZooKeeper badzk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            //授权
            badzk.addAuthInfo(authentication_type, badAuthentication.getBytes());
            Thread.sleep(2000);
            System.out.println(prefix + "获取数据：" + PATH);
            System.out.println(prefix + "成功获取数据：" + badzk.getData(PATH, false, null));
        } catch (Exception e) {
            System.err.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    /**
     * 获取数据：不采用密码
     */
    static void getDataByNoAuthentication() {
        String prefix = "[不使用任何授权信息]";
        try {
            System.out.println(prefix + "获取数据：" + PATH);
            ZooKeeper nozk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            Thread.sleep(2000);
            System.out.println(prefix + "成功获取数据：" + nozk.getData(PATH, false, null));
        } catch (Exception e) {
            System.err.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    /**
     * 采用正确的密码
     */
    static void getDataByCorrectAuthentication() {
        String prefix = "[使用正确的授权信息]";
        try {
            System.out.println(prefix + "获取数据：" + PATH);
            System.out.println(prefix + "成功获取数据：" + zk.getData(PATH, false, null));
        } catch (Exception e) {
            System.out.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    /**
     * 更新数据：不采用密码
     */
    static void updateDataByNoAuthentication() {

        String prefix = "[不使用任何授权信息]";

        System.out.println(prefix + "更新数据： " + PATH);
        try {
            ZooKeeper nozk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            Thread.sleep(2000);
            Stat stat = nozk.exists(PATH, false);
            if (stat != null) {
                nozk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix + "更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "更新失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 更新数据：采用错误的密码
     */
    static void updateDataByBadAuthentication() {
        String prefix = "[使用错误的授权信息]";
        System.out.println(prefix + "更新数据：" + PATH);
        try {
            ZooKeeper badzk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            //授权
            badzk.addAuthInfo(authentication_type, badAuthentication.getBytes());
            Thread.sleep(2000);
            Stat stat = badzk.exists(PATH, false);
            if (stat != null) {
                badzk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix + "更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "更新失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 更新数据：采用正确的密码
     */
    static void updateDataByCorrectAuthentication() {
        String prefix = "[使用正确的授权信息]";
        System.out.println(prefix + "更新数据：" + PATH);
        try {
            Stat stat = zk.exists(PATH, false);
            if (stat != null) {
                zk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix + "更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "更新失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 不使用密码 删除节点
     */
    static void deleteNodeByNoAuthentication() throws Exception {
        String prefix = "[不使用任何授权信息]";
        try {
            System.out.println(prefix + "删除节点：" + PATH_DEL);
            ZooKeeper nozk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            Thread.sleep(2000);
            Stat stat = nozk.exists(PATH_DEL, false);
            if (stat != null) {
                nozk.delete(PATH_DEL, -1);
                System.out.println(prefix + "删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "删除失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 采用错误的密码删除节点
     */
    static void deleteNodeByBadAuthentication() throws Exception {
        String prefix = "[使用错误的授权信息]";
        try {
            System.out.println(prefix + "删除节点：" + PATH_DEL);
            ZooKeeper badzk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            //授权
            badzk.addAuthInfo(authentication_type, badAuthentication.getBytes());
            Thread.sleep(2000);
            Stat stat = badzk.exists(PATH_DEL, false);
            if (stat != null) {
                badzk.delete(PATH_DEL, -1);
                System.out.println(prefix + "删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "删除失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 使用正确的密码删除节点
     */
    static void deleteNodeByCorrectAuthentication() throws Exception {
        String prefix = "[使用正确的授权信息]";
        try {
            System.out.println(prefix + "删除节点：" + PATH_DEL);
            Stat stat = zk.exists(PATH_DEL, false);
            if (stat != null) {
                zk.delete(PATH_DEL, -1);
                System.out.println(prefix + "删除成功");
            }
        } catch (Exception e) {
            System.out.println(prefix + "删除失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 使用正确的密码删除节点
     */
    static void deleteParent() throws Exception {
        try {
            Stat stat = zk.exists(PATH_DEL, false);
            if (stat == null) {
                zk.delete(PATH, -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

