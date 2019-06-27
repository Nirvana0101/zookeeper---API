package com.momo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * zk节点增读改删操作类
 */
public class ZkNodeAclOperate implements Watcher {
    private ZooKeeper zooKeeper=null;
    final static Logger log = LoggerFactory.getLogger(ZkConnect.class);
    private static final String zkServerPath = "114.115.215.115:2181";
    private static final Integer tiomeOut=5000;
    private static Stat stat = new Stat();
    private static CountDownLatch countDown = new CountDownLatch(1);
    public ZkNodeAclOperate() {
    }
    public ZkNodeAclOperate(String zkServerPath) throws IOException {
        zooKeeper=new ZooKeeper(zkServerPath,tiomeOut,new ZkNodeAclOperate());}
    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    /**
     * 创建节点并自定义用户访问权限
     */
    public void  createZkNodeAndSetAcl(String path,byte[] data ,List<ACL> acls ) throws Exception {
        zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
    }

    public static void main(String[] args) throws Exception {
        ZkNodeAclOperate zkNodeAclOperate = new ZkNodeAclOperate(zkServerPath);
        /**
         * 创建节点并自定义用户访问权限
        List<ACL> acls = new ArrayList<ACL>();
        Id momo1 = new Id("digest", AclUtils.getDigestUserPwd("momo1:momo1"));
        Id momo2 = new Id("digest", AclUtils.getDigestUserPwd("momo2:momo2"));
        acls.add(new ACL(ZooDefs.Perms.ALL, momo1));
        acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, momo2));
        zkNodeCrudOperate.createZkNodeAndSetAcl("/momo1","momo1".getBytes(),acls);
        */

        /**
         * 登录后操作节点
        zkNodeAclOperate.getZooKeeper().addAuthInfo("digest", "momo1:momo1".getBytes());
        Stat stat = new Stat();
        byte[] data = zkNodeAclOperate.getZooKeeper().getData("/momo1", false, stat);
        System.out.println(new String(data));
        System.out.println(stat.getVersion());
        */

        /**
         *创建节点并自定义这个节点可以访问的ip
         */
        List<ACL> aclsIP = new ArrayList<ACL>();
		Id ipId= new Id("ip", "172.16.0.11");
		aclsIP.add(new ACL(ZooDefs.Perms.ALL, ipId));
        zkNodeAclOperate.createZkNodeAndSetAcl("/172.16.0.11", "172.16.0.11".getBytes(), aclsIP);
        // 验证ip是否有权限
//        Stat stat1 = new Stat();
//        byte[] data = zkNodeAclOperate.getZooKeeper().getData("/test10.187.16.112", false, stat);
//        System.out.println(new String(data));
//        System.out.println(stat.getVersion());
    }
}
