package com.renren.bigdata.com.renren.test;

import org.apache.hadoop.conf.Configuration;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-16
 * Time: 下午5:38
 * To change this template use File | Settings | File Templates.
 */
public class ScanThread extends Thread{
    Set<String> set;
    Configuration conf;
    String table;
    String id;
    public ScanThread(Set set, Configuration conf, String table, String id){
        this.set = set;
        this.conf = conf;
        this.table = table;
        this.id = id;
    }



}
