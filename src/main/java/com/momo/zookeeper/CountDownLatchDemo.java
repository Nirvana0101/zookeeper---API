package com.momo.zookeeper;

import java.util.concurrent.CountDownLatch;

/**
 * 计数器代码演示
 */
public class CountDownLatchDemo {
    public static void main(String[] args) {
        CountDownLatch countDownLatch=new CountDownLatch(8);//全局控制的计数器
        new Thread(()->{
            try {
                countDownLatch.await();//线程阻塞，等到count的数字全部减完才往下执行
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("800米比赛结束，准备清空跑道并继续跨栏比赛");
        }
        ).start();
        for(int i=0;i<8;i++){
            int ii=i;
            new Thread(()->{
                try {
                    Thread.sleep(ii * 1000L);
                    System.out.println(Thread.currentThread().getName()+"到达终点");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown(); //每执行一次减1
                }
            }
            ).start();
        }
    }
}
