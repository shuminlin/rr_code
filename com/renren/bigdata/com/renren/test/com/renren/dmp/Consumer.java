package com.renren.bigdata.com.renren.test.com.renren.dmp;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.System.out;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-18
 * Time: 下午4:01
 * To change this template use File | Settings | File Templates.
 */
public class Consumer implements Runnable{
    private volatile boolean processFinished = false;
    private LinkedBlockingQueue<String> queue;

    public Consumer(LinkedBlockingQueue<String> queue){
        this.queue = queue;
    }

    public void run(){
        int count = 0;
        while (!processFinished || !queue.isEmpty()){
            String value = queue.poll();
            if(value != null)count++;

        }

//        out.println(Thread.currentThread().getId() + " : " + count);
        out.println(count);


    }
    public void notifyProcessOver(){
        this.processFinished = true;
    }
}
