package com.renren.bigdata.com.renren.test.com.renren.dmp;

import com.renren.bigdata.TypeCode;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.System.out;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-18
 * Time: 下午4:01
 * To change this template use File | Settings | File Templates.
 */
public class PutThread implements Runnable{
    private volatile boolean processFinished = false;
    private LinkedBlockingQueue<String> queue;
    private HTablePool pool;
    private CountDownLatch countDownLatch;

    public PutThread(CountDownLatch countDownLatch,LinkedBlockingQueue<String> queue,HTablePool pool){
        this.countDownLatch = countDownLatch;
        this.queue = queue;
        this.pool = pool;
    }

    public void run(){
        int count = 0;
        HTableInterface table = pool.getTable("poc_id_hub");
        IDPut idPut = new IDPut(table);

        while (!processFinished || !queue.isEmpty()){
            String value = queue.poll();
            if(value == null) continue;
//            out.println(Thread.currentThread().getName()+": got " +value);
            count ++;

            LinkedList<Put> puts = null;
            try {
                puts = idPut.getPuts(value);
                table.put(puts);
                table.flushCommits();

            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }


        }
        try {
            pool.putTable(table);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        out.println(Thread.currentThread().getName()+": task count = " +count);
        countDownLatch.countDown();


        }

//        out.println(Thread.currentThread().getId() + " : " + count);
//        out.println(count);



    public void notifyProcessOver(){
        this.processFinished = true;
    }
}
