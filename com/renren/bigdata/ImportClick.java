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
public class ImportClick {



    public static void main(String[] args) throws Exception{
//        String INPUT_FILE = "/Users/zyshen/app_ad_stats";
        String INPUT_FILE = "/Volumes/Elements/456_app_ad_stats";
        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"test_user_activity");
        table.setAutoFlush(false);


        long start = System.currentTimeMillis();

        String line = null;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            List<Put> puts = new ArrayList<Put>();
            String[] value = line.split("\t");
//            System.out.println("got a line, length = "+value.length);

            if(value.length == 4){//time + uuid1 + AppName + AD_ID
            //construct
                String time = value[0];
                String uuid1 = value[1];
                String appName = value[2];
                String adId = value[3];

                uuid1 = TypeCode.IOS_UUID1+"\u0000"+uuid1;

                Put visitPut = new Put(Bytes.toBytes(uuid1+"\u0001"+time));
                Put clickPut = new Put(Bytes.toBytes(uuid1+"\u0001"+time));

                visitPut.add(Bytes.toBytes("a"),Bytes.toBytes("visit"),Bytes.toBytes(appName));
                clickPut.add(Bytes.toBytes("a"),Bytes.toBytes("click"),Bytes.toBytes(adId));

                puts.add(visitPut);
                puts.add(clickPut);

                for(Put put:puts) {
                    put.setWriteToWAL(false);
                    count++;
                }

            }

            if(count%10000 == 0) System.out.println("count  = "+count +" time used: "+(System.currentTimeMillis()-start)+" ms.");

            if(value.length!=4){
                System.err.println("Error data encountered!! "+line);
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
