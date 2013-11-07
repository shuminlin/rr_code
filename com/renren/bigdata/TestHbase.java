package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.SecurityUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-7-29
 * Time: 下午3:16
 * To change this template use File | Settings | File Templates.
 */
public class TestHbase {
    public static void testPut(Configuration conf) throws Exception{

        HTable table = new HTable(conf,"test_id_hub");
        List<Put> puts;
        Put put;
//        puts = new ArrayList<Put>();

//        for (int i=0;i<10;i++){
//            Put put = new Put(Bytes.toBytes("uuid_"+i));
//            put.add(Bytes.toBytes("l"),Bytes.toBytes("gender.male.prop"),Bytes.toBytes(""+Math.random()));
////            table.put(put);
//            puts.add(put);
//        }

        put = new Put(Bytes.toBytes("1"+"/U0001"+"UUID3"));
        put.add(Bytes.toBytes("i"),Bytes.toBytes("q"),Bytes.toBytes(""));
        table.put(put);

        table.flushCommits();

        table.close();

    }

    public static void testGet(Configuration conf)throws Exception{
        HTable table = new HTable(conf,"test_user_label");
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("gender`"));

        Get get = new Get(Bytes.toBytes("uuid_8"));
//        get.addColumn(Bytes.toBytes("l"),Bytes.toBytes("gender.male.prop"));

        get.setMaxVersions(3);
        get.setFilter(filter);
        Result result = table.get(get);


        System.out.println("Got "+result.size()+ " cells!");
        System.out.println("Dump: "+ result.toString());
        System.out.println("Row: "+ Bytes.toString(result.getRow()));

        System.out.println(result.containsColumn(Bytes.toBytes("l"),Bytes.toBytes("gender.female")));

    }

    public static void testGets(Configuration conf)throws Exception{
        HTable table = new HTable(conf,"test_user_label");
        List<Get> gets;
        gets = new ArrayList<Get>();
        Get get1,get2;
        get1 = new Get(Bytes.toBytes("uuid_7"));
        get2 = new Get(Bytes.toBytes("uuid_8"));

        gets.add(get1);
        gets.add(get2);

        Result[] results = table.get(gets);

        for(Result result:results){
            System.out.println(Bytes.toString(result.getRow())+ "  " +result.toString());
        }


    }


    public static void testScan(Configuration conf)throws Exception{
        HTable table = new HTable(conf,"test_id_hub");
        int count =0;
//        byte[] startKey = Bytes.add(Bytes.toBytes(TypeCode.MAIL),Bytes.toBytes("\u0000"),Bytes.toBytes("qiurui0516@sohu.com"));
        byte[] startKey = Bytes.toBytes(TypeCode.ID);

        byte[] endKey = Bytes.toBytes(Bytes.toString(startKey)+"\u0001");

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
//        scan.setStartRow(startKey);
//        scan.setStopRow(endKey);
//        scan.setStartRow( Bytes.toBytes("uuid_8"));                   // start key is inclusive
//        scan.setStopRow( Bytes.toBytes("uuid_8"+"\uffff"));  // stop key is exclusive

//        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("gender"));
//        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        for(Result result:scanner){
            System.out.println(Bytes.toString(result.getRow())+" "+result.toString());
            String[] v =  Bytes.toString(result.getRow()).split("\u0001");
            String[] v0 = v[0].split("\u0000");
            String[] v1 = v[1].split("\u0000");
            System.out.println("mail:  type="+v0[0] +"  id="+v0[1]);
            System.out.println("renren: type="+v1[0]+"  id"+v1[1]);
            count++;
        }

        System.out.println("Start key = "+Bytes.toString(startKey)+"\nEnd key = "+Bytes.toString(endKey)+"\nTotal = "+count);
        System.out.println("\nTotal = "+count);

//        table.exists()

    }

    public static void main(String[] ar) throws Exception{
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");


        SecurityUtil.login(conf,"/Users/zyshen/work/hbase.keytab","hbase@H.DATA.GAME.OPI.COM");

        // do test:
        long start = System.currentTimeMillis();
//        testPut(conf);
//        testGet(conf);
//        testGets(conf);
        testScan(conf);
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }
}
