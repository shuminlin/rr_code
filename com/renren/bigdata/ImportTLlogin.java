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
public class ImportTLlogin {
    public static void main(String[] args) throws Exception{
//        String INPUT_FILE = "/Users/zyshen/Downloads/t_rrid_uuid";
        String INPUT_FILE = "/Users/zyshen/work/data/ane_userid_uuid1";

        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"poc_id_hub");
        table.setAutoFlush(false);



        String line = null;

        int count = 0;
        int badCount = 0;
        long start = System.currentTimeMillis();

        while ((line = reader.readLine()) != null) {
            List<Put> puts = new ArrayList<Put>();
            count++;
            if (count%10000 == 0)System.out.println("count = "+count);
            String[] value = line.split("\\t");
//            System.out.println("got a line, length = "+value.length);

            if(value.length == 2){//rrid + uuid
            //construct
                String rrId = value[0];
                String uuId = value[1];

                if(uuId.length()!="42ca863fc0604d849d95d33491f9f63a".length()) {
//                    System.out.println("Bad UUID1 --- "+uuId);
                    badCount++;
                    continue;
                }

                Put put1 = new Put(HBaseUtil.compositeKey(TypeCode.RENREN_ID,rrId,TypeCode.IOS_UUID1,uuId));
                put1.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));

                Put put2 = new Put(HBaseUtil.compositeKey(TypeCode.IOS_UUID1,uuId,TypeCode.RENREN_ID,rrId));
                put2.add(Bytes.toBytes("i"),Bytes.toBytes("i"),Bytes.toBytes(""));

                puts.add(put1);
                puts.add(put2);
                for(Put put:puts){
                    put.setWriteToWAL(false);
                }
                table.put(puts);

            }

            if(value.length!=2){
                badCount++;
                System.out.println("Error data encountered!! "+line);
            }
            }
        table.flushCommits();
        table.close();
        System.out.println("\n------------Summary---------------");
        System.out.println("Total = "+count + " Bad Count = "+badCount);
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");


//            System.out.println("1---"+value[0]+" 2---"+value[1]+" 3---"+value[2]);
//            table.setWriteBufferSize(1024*1024*10);


        }


    }

