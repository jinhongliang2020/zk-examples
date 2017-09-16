package org.hong.zkclient;

import org.I0Itec.zkclient.IZkDataListener;


/**
 * 订阅节点的数据内容的变化
 * Created by hong on 2017/9/7.
 */
public class ZkDataListener implements IZkDataListener{


    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
        System.out.println(dataPath+":"+data.toString());
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
        System.out.println(dataPath);
    }
}
