package com.renren.bigdata.com.renren.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SecurityUtil;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-19
 * Time: 上午9:37
 * To change this template use File | Settings | File Templates.
 */
public class TestCounters {

    public static void main(String[] args)throws Exception{
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");


        SecurityUtil.login(conf,"/Users/zyshen/work/hbase.keytab","hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"test_counters");
        table.setAutoFlush(false);

        long start = System.currentTimeMillis();
        //do your job here
//        for(int i=0;i<1000;i++){
//            Increment inc = new Increment(Bytes.toBytes("adrenrenad"));
//            inc.setWriteToWAL(false);
//            inc.addColumn(Bytes.toBytes("c"),Bytes.toBytes("sum"),1);
//            table.increment(inc);
////            table.incrementColumnValue(Bytes.toBytes("ad:renrenad"),Bytes.toBytes("c"),Bytes.toBytes("sum"),(long)1);
//        }

        Increment inc = new Increment(Bytes.toBytes("adrenrenad"));
        inc.addColumn(Bytes.toBytes("c"),Bytes.toBytes("sum"),0);
        table.increment(inc);
        TimeRange tr = inc.getTimeRange();

        System.out.println("[tr]"+tr);

        //job ending
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");

    }
}
