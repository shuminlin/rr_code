package com.renren.bigdata.com.renren.test.com.renren.dmp;

import com.renren.bigdata.TypeCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.security.SecurityUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-18
 * Time: 下午4:01
 * To change this template use File | Settings | File Templates.
 */
public class GetQQLink {
    public static void main(String[] args)throws Exception{
        String fileName = "/Users/zyshen/rrid_qq";

        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");
        Configuration conf = HBaseConfiguration.create();
        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");
        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        String tableName = "test_id_hub";
        HTablePool pool = new HTablePool(conf,10);
        HTableInterface table = pool.getTable("test_id_hub");



        long start = System.currentTimeMillis();
        IDPut idPut = new IDPut(table);
        LinkedList<String> list = null;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;

            while ((line = reader.readLine())!=null){
                String rrid = line.split("\t")[0];
                String qq = line.split("\t")[1];
                list = idPut.getIDs(TypeCode.QQ+qq);
                if(list.size()>=3) {
                    System.out.println("["+list.size()+"]"+qq);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }



        System.out.println("File read finished!");
        table.close();
        pool.close();
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");


    }

}
