package com.renren.bigdata.com.renren.test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-18
 * Time: 下午2:29
 * To change this template use File | Settings | File Templates.
 */
public class TestThread extends Thread{
    Set<String> data;
    CountDownLatch cd;
    public TestThread(){

    }
    public TestThread(Set data, CountDownLatch cd){
        this.data = data;
        this.cd = cd;
    }
    public void run(){
        System.out.println("Thread "+this.getName()+" has started!");
        for(int i=0;i<10;i++){
           data.add(this.getId() + "" + i);
        }
        for(String val:data){
            System.out.println("from:"+this.getName()+"--"+val);
        }
        cd.countDown();
    }
}
