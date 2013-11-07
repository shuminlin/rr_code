package com.renren.bigdata.com.renren.test.com.renren.dmp;

import com.renren.bigdata.TypeCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SecurityUtil;

import java.util.ArrayList;
import java.util.LinkedList;

import static com.renren.bigdata.TypeCode.IOS_UUID1;
import static com.renren.bigdata.TypeCode.RENREN_ID;
import static java.lang.System.in;
import static java.lang.System.out;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-22
 * Time: 下午3:00
 * To change this template use File | Settings | File Templates.
 */
public class IDPut {
    private HTableInterface table;

    public IDPut(HTableInterface table){
        this.table = table;
    }

    public LinkedList<String> getIDs(String id) throws Exception{
        LinkedList<String> results = new LinkedList<String>();
        int count = 0;
        byte[] startKey = Bytes.toBytes(id);
        byte[] endKey = Bytes.toBytes(id+"\u0002");

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);

        ResultScanner scanner = table.getScanner(scan);

        for(Result result:scanner){
            String v =  (Bytes.toString(result.getRow()).split("\u0001")[1]);
            results.add(v);
            count++;
        }
//        if(count >= 100) out.println("Thread ID:"+Thread.currentThread().getName()+" --- Too many linked data: "+count +", id = "+id);

        scanner.close();
        return results;
    }

    public LinkedList<Put> getPuts(String input) throws Exception{
        LinkedList<Put> puts = new LinkedList<Put>();
        LinkedList<String> id1List ;
        LinkedList<String> id2List ;

        String id1 = input.split("\u0001")[0];
        String id2 = input.split("\u0001")[1];

        if (id1.equals(id2)) {
            throw new IllegalArgumentException("id1 == id2");
        }

        id1List = getIDs(id1);
        id2List = getIDs(id2);

        puts.add(new Put(Bytes.toBytes(id1+"\u0001"+id2)));
        puts.add(new Put(Bytes.toBytes(id2+"\u0001"+id1)));


        for (String tmp:id1List){
            if(!tmp.equals(id2)){
                puts.add(new Put(Bytes.toBytes(tmp+"\u0001"+id2)));
                puts.add(new Put(Bytes.toBytes(id2+"\u0001"+tmp)));
            }
        }

        for (String tmp:id2List){
            if(!tmp.equals(id1)){
                puts.add(new Put(Bytes.toBytes(tmp+"\u0001"+id1)));
                puts.add(new Put(Bytes.toBytes(id1+"\u0001"+tmp)));
            }
        }

        for(Put put:puts){
            put.setWriteToWAL(false);
            put.add(Bytes.toBytes("i"),Bytes.toBytes("i"),Bytes.toBytes(""));
        }

        return puts;
    }

    public static void main(String[] args) throws Exception{
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");
        Configuration conf = HBaseConfiguration.create();
        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");
        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        String tableName = "poc_id_hub";
        HTablePool pool = new HTablePool(conf,100);
        HTableInterface table = pool.getTable(tableName);
        table.setAutoFlush(false);

        IDPut p =  new IDPut(table);

//        LinkedList<String> results = p.getIDs( IOS_UUID1+"\u0000"+"35e98bb492f44726a61f960f7e63642e");
//        LinkedList<String> results = p.getIDs( RENREN_ID+"\u0000"+"549688942");

        String str = TypeCode.QQ+"\u0000"+"363342387";
        LinkedList<String> results = p.getIDs(str);



        for (String tmp:results){
            out.println("type: "+tmp.split("\u0000")[0]+"\t id: "+tmp.split("\u0000")[1]);
        }

//        LinkedList<Put> puts = p.getPuts(TypeCode.QQ+"\u0000"+"12345"+"\u0001"+TypeCode.RENREN_ID+"\u0000"+"88888");
//
//        for(Put put:puts){
//            out.println(put.toJSON());
//        }
//
//        table.put(puts);
//        table.flushCommits();

        //clean:
        pool.close();
    }
}
