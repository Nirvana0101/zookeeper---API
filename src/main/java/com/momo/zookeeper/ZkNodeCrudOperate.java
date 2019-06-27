package com.momo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * zk节点增读改删操作类
 */
public class ZkNodeCrudOperate implements Watcher {
    private ZooKeeper zooKeeper=null;
    final static Logger log = LoggerFactory.getLogger(ZkConnect.class);
    private static final String zkServerPath = "114.115.215.115:2181";
    private static final Integer tiomeOut=5000;
    private static Stat stat = new Stat();
    private static CountDownLatch countDown = new CountDownLatch(1);
    public ZkNodeCrudOperate() {
    }
    public ZkNodeCrudOperate(String zkServerPath) throws IOException {
        zooKeeper=new ZooKeeper(zkServerPath,tiomeOut,new ZkNodeCrudOperate());}
    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeDataChanged){
            try {
                zooKeeper=new ZooKeeper(zkServerPath,tiomeOut,new ZkNodeCrudOperate());
                byte[] result=zooKeeper.getData("/hello",true,stat);
                System.out.println("更改后的值:" + new String(result));
                System.out.println("版本号变化dversion：" + stat.getVersion());
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            countDown.countDown();
        }else if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
            try {
                zooKeeper=new ZooKeeper(zkServerPath,tiomeOut,new ZkNodeCrudOperate());
                List<String> strChildList = zooKeeper.getChildren(watchedEvent.getPath(), false);
                for (String s : strChildList) {
                    byte[] result=zooKeeper.getData("/hello/"+s,false,stat);
                    System.out.println(s+":====>>"+new String(result));
                }
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            countDown.countDown();
        }if (watchedEvent.getType() == Event.EventType.NodeCreated) {
            System.out.println("节点创建");
            countDown.countDown();
        } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            System.out.println("节点数据改变");
            countDown.countDown();
        } else if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            System.out.println("节点删除");
            countDown.countDown();
        }
    }
    /**
     * 同步方式创建zk节点,并默认匿名用户权限
     */
    public void createZkNode(String path,byte[] data ,List<ACL>acls){
        try {
            String result = zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
            System.out.println("创建节点：\t" + result + "\t成功...");
            new Thread().sleep(2000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * 异步方式创建zk节点,并默认匿名用户权限
     */
    public void createCallBackNode(String path,byte[] data ,List<ACL>acls){
        try {
            String ctx = "{'create':'success'}";
            zooKeeper.create(path, data, acls, CreateMode.PERSISTENT, new CreateCallBack(), ctx);
            new Thread().sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *修改节点
     * @throws IOException
     */
    public void modifyNode(String path,byte[] data ,int version){
        try {
            Stat result= zooKeeper.setData(path, data, version );
            new Thread().sleep(2000);
            System.out.println("修改节点，版本号为：\t" + result.getVersion() + "\t成功...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    /**
     * 同步方式删除zk节点
     */
    public void deleteZkNode(String path,int version){
        try {
            zooKeeper.delete(path,version);
            System.out.println("删除节点：\t"  + "\t成功...");
            new Thread().sleep(2000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * 异步方式创建zk节点
     */
    public void deleteCallBackNode(String path,int version){
        try {
            String ctx = "{'delete':'success'}";
            zooKeeper.delete(path,version, new DeleteCallBack(), ctx);
            new Thread().sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步读取节点数据
     * @param path
     */
    public  void getNodeData(String path){
        try {
            byte[] result=zooKeeper.getData(path,true,stat);
            System.out.println(path+"节点的的值为："+new String(result));
            countDown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    /**
     * 同步读取节点数据
     * @param path
     */
    public  void getChildrenData(String path){
        try {
            List<String> list=zooKeeper.getChildren(path,true);
		for (String s : list) {
			System.out.println("当前子节点："+s);
		}
            countDown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    /**
     * 判断用户是否存在
     * @param path
     */
    public  void zKnodeExists(String path){
        try {
            Stat stat =zooKeeper.exists(path,true);
            if (stat != null) {
                System.out.println("查询的节点版本为dataVersion：" + stat.getVersion());
            } else {
                System.out.println("该节点不存在...");
            }
            countDown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        ZkNodeCrudOperate zkNodeCrudOperate=new ZkNodeCrudOperate(zkServerPath);
        /**
         * 同步方式创建zk节点
         */
//       zkNodeCrudOperate.createZkNode("/helloworld","helloworld".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        /**
         * 异步方式创建zk节点
         */
//        zkNodeCrudOperate.createCallBackNode("/createCallBackNode","createCallBackNode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        /**
         * 修改zk节点，version为原版本号
         */
//        zkNodeCrudOperate.modifyNode("/helloworld","modify".getBytes(),0);
        /**
         * 同步删除zk节点，version为原版本号
         */
//        zkNodeCrudOperate.deleteZkNode("/helloworld",1);
        /**
         * 异步删除zk节点，version为原版本号
         */
//        zkNodeCrudOperate.deleteCallBackNode("/createCallBackNode",0);
        /**
         * 同步读取zk节点
         */
//        zkNodeCrudOperate.getNodeData("/hello");
        /**
         *获取子节点列表
         */
//        zkNodeCrudOperate.getChildrenData("/hello");
        /**
         *查询节点是否存在,如果节点不存在又设置了监听，那么必须新创建的节点名称必须一样才会触发watch事件
         */
        zkNodeCrudOperate.zKnodeExists("/hello2");
    }
}
