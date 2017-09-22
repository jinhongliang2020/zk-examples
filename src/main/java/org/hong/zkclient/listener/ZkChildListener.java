package org.hong.zkclient.listener;

import org.I0Itec.zkclient.IZkChildListener;

import java.util.List;

/**
 *  订阅节点的信息改变（创建节点，删除节点，添加子节点）
 *  其实就是Watcher 的实现.
 *  Created by hong on 2017/9/7.
 */
public class ZkChildListener  implements IZkChildListener {
    /**
     * handleChildChange： 用来处理服务器端发送过来的通知
     * parentPath：对应的父节点的路径
     * currentChilds：子节点的相对路径
     */
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {

        System.out.println(""+parentPath);
        System.out.println(currentChilds.toString());

    }
}
