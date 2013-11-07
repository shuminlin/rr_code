package com.renren.bigdata.com.renren.test.com.renren.dmp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-18
 * Time: 下午4:01
 * To change this template use File | Settings | File Templates.
 */
public class Producer implements Runnable{
    private LinkedBlockingQueue<String> queue;
    private String fileName;
    private boolean finshFlag = false;

    public Producer(String fileName, LinkedBlockingQueue<String> queue, boolean finishFlag){
        this.fileName = fileName;
        this.queue = queue;
    }
    public void run(){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine())!=null){
                queue.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


    }

}
