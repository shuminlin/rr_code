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
public class ImportUserLabel {
    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/Downloads/rrid_uuid1.csv";
        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"test_id_hub");
        List<Put> puts = new ArrayList<Put>();

        String line = null;

        while ((line = reader.readLine()) != null) {
            String[] value = line.split("\\,");
//            System.out.println("got a line, length = "+value.length);

            if(value.length == 2){//rrid + mail
            //construct
                String rrId = value[0];
                String mailId = value[1];

                Put put1 = new Put(HBaseUtil.compositeKey(TypeCode.MAIL,mailId,TypeCode.RENREN_ID,rrId));
                put1.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));

                Put put2 = new Put(HBaseUtil.compositeKey(TypeCode.RENREN_ID,rrId,TypeCode.MAIL,mailId));
                put2.add(Bytes.toBytes("i"),Bytes.toBytes("i"),Bytes.toBytes(""));

                puts.add(put1);
                puts.add(put2);

            }
            if(value.length == 3){//rrid + mail + id
                String rrId = value[0];
                String mailId = value[1];
                String id = value[2];

                Put put1 = new Put(HBaseUtil.compositeKey(TypeCode.MAIL,mailId,TypeCode.RENREN_ID,rrId));
                put1.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));

                Put put2 = new Put(HBaseUtil.compositeKey(TypeCode.RENREN_ID,rrId,TypeCode.MAIL,mailId));
                put2.add(Bytes.toBytes("i"),Bytes.toBytes("i"),Bytes.toBytes(""));

                Put put3 = new Put(HBaseUtil.compositeKey(TypeCode.ID,id,TypeCode.RENREN_ID,rrId));
                put3.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));

                Put put4 = new Put(HBaseUtil.compositeKey(TypeCode.RENREN_ID,rrId,TypeCode.ID,id));
                put4.add(Bytes.toBytes("i"),Bytes.toBytes("i"),Bytes.toBytes(""));

                Put put5 = new Put(HBaseUtil.compositeKey(TypeCode.ID,id,TypeCode.MAIL,mailId));
                put5.add(Bytes.toBytes("i"), Bytes.toBytes("i"), Bytes.toBytes(""));

                Put put6 = new Put(HBaseUtil.compositeKey(TypeCode.MAIL,mailId,TypeCode.ID,id));
                put6.add(Bytes.toBytes("i"),Bytes.toBytes("i"),Bytes.toBytes(""));

                puts.add(put1);
                puts.add(put2);
                puts.add(put3);
                puts.add(put4);
                puts.add(put5);
                puts.add(put6);




            }

            if(value.length<2 || value.length>3){
                System.err.println("Error data encountered!! "+line);
            }

//            System.out.println("1---"+value[0]+" 2---"+value[1]+" 3---"+value[2]);


        }
        table.put(puts);
        table.flushCommits();
        table.close();
    }

}
