package org.hong.zkclient.listener;

import org.I0Itec.zkclient.IZkDataListener;


/**
 * 订阅节点的数据内容的变化
 * Created by hong on 2017/9/7.
 */
public class ZkDataListener implements IZkDataListener{


    public void handleDataChange(String dataPath, Object data) throws Exception {
        System.out.println(dataPath+":"+data.toString());
    }

    public void handleDataDeleted(String dataPath) throws Exception {
        System.out.println(dataPath);
    }
}
