package com.renren.bigdata.com.renren.test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import static java.lang.System.*;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-16
 * Time: 下午5:38
 * To change this template use File | Settings | File Templates.
 */
public class TestMain {
    public static void main(String[] args) throws Exception{
//        CountDownLatch cd = new CountDownLatch(2);
//        Vector<String> vector = new Vector<String>();
//        Set<String> set;
//        set = Collections.synchronizedSet(new HashSet<String>());
//
//        TestThread t1 = new TestThread(set,cd);
//        TestThread t2 = new TestThread(set,cd);
//
//        t1.start();
//        t2.start();
//
//        set.add("main");
//
//        cd.await();
////        Thread.sleep(2000);
//        for(String val:set){
//            System.out.println("[main]"+val);
//        }

//        System.out.println("interest. 7".substring(10));
        Thread t1 = new Thread (new TimePrinter(1000, "1 Fast Guy"));
        t1.start();
        Thread t2 = new Thread (new TimePrinter(3000, "2 Slow Guy"));
        t2.start();
        out.println("hello dead!");

    }
}
