package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
public class ImportCivilID {



    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/Downloads/t_user_info.csv";
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

            if(value.length == 3){//rrid + mail + civil id
            //construct
                String rrId = value[0];
                String civilId = value[2];

                rrId = TypeCode.RENREN_ID+"\u0000"+rrId;
                civilId = TypeCode.IOS_UUID1+"\u0000"+civilId;

                List<String> rrIdNeighbors = HBaseUtil.getNeighbor(rrId,conf,"test_id_hub");
                List<String> civilIdNeighbors = HBaseUtil.getNeighbor(civilId,conf,"test_id_hub");

                Put[] putList;

                putList = HBaseUtil.key2puts(rrId,civilId);
                for(Put put:putList) puts.add(put);

                for(String rrIdNeighbor:rrIdNeighbors){
                    if(rrIdNeighbor.equals(civilId)) continue;
                    putList = HBaseUtil.key2puts(civilId,rrIdNeighbor);
                    for(Put put:putList) puts.add(put);
                }

                for(String civiIdNeighbor:civilIdNeighbors){
                    if(civiIdNeighbor.equals(rrId)) continue;
                    putList = HBaseUtil.key2puts(rrId,civiIdNeighbor);
                    for(Put put:putList) puts.add(put);
                }

                for(Put put:puts) {
                    put.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));
                    put.setWriteToWAL(false);
                    count++;
                    if(count%1000 == 0) System.out.println("count  = "+count +" time used: "+(System.currentTimeMillis()-start)+" ms.");
                }

            }

//            if(value.length!=2){
//                System.err.println("Error data encountered!! "+line);
//            }

            table.put(puts);

        }
        table.flushCommits();
        table.close();

        System.out.println("\n------------Summary---------------");
        System.out.println("Total = "+count);
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }

}
