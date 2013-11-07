package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SecurityUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-9
 * Time: 上午10:57
 * To change this template use File | Settings | File Templates.
 */
public class ImportTMP {



    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/t_login.txt";
        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"test_id_hub");
        table.setAutoFlush(false);

        long start = System.currentTimeMillis();

        String line = null;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            List<Put> puts = new ArrayList<Put>();
            String[] value = line.split("\\,");
//            System.out.println("got a line, length = "+value.length);

            if(value.length == 2){//rrid + uuid1
            //construct
                String rrId = value[0];
                String uuid1 = value[1];

                rrId = TypeCode.RENREN_ID+"\u0000"+rrId;
                uuid1 = TypeCode.IOS_UUID1+"\u0000"+uuid1;

//                List<String> rrIdNeighbors = HBaseUtil.getNeighbor(rrId,conf,"test_id_hub");
//                List<String> uuid1Neighbors = HBaseUtil.getNeighbor(mailID,conf,"test_id_hub");

                Put[] putList;

                putList = HBaseUtil.key2puts(rrId,uuid1);
                for(Put put:putList) puts.add(put);

//                for(String rrIdNeighbor:rrIdNeighbors){
//                    putList = HBaseUtil.key2puts(mailID,rrIdNeighbor);
//                    for(Put put:putList) puts.add(put);
//                }
//
//                for(String uuid1Neighbor:uuid1Neighbors){
//                    putList = HBaseUtil.key2puts(rrId,uuid1Neighbor);
//                    for(Put put:putList) puts.add(put);
//                }

                for(Put put:puts) {
                    put.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));
                    put.setWriteToWAL(false);
                    count++;
                    if(count%10000 == 0) System.out.println("count  = "+count +" time used: "+(System.currentTimeMillis()-start)+" ms.");
                }

            }

            if(value.length!=2){
//                System.err.println("Error data encountered!! "+line);
            }

            table.put(puts);

        }
        table.flushCommits();
        table.close();

        System.out.println("\n------------Summary---------------");
        System.out.println("Total = "+count);
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }

}
