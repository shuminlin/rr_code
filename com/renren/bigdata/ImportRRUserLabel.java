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
public class ImportRRUserLabel {
    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/t_user_info_all";
        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"test_user_label");
        table.setAutoFlush(false);


        String line = null;

        long start =  System.currentTimeMillis();
        int count = 0;
        int count4=0;
        int count3=0;
        int count2=0;


        while ((line = reader.readLine()) != null) {
            List<Put> puts = new ArrayList<Put>();
            String[] value = line.split("\t");
//            System.out.println("got a line, length = "+value.length);

            if(value.length == 4){//rrid + mail + truename + idcard
                count4++;
                String rrId = value[0];
                String mail = value[1];
                String trueName = value[2];
                String idCard = value[3];

//                System.out.println("4--"+value[0]+value[1]+value[2]+value[3]);

//                if(mail != null && mail.contains("@")){
//                    Put mailPut  = new Put(Bytes.toBytes(TypeCode.RENREN_ID+"\u0000"+rrId));
//                    mailPut.add(Bytes.toBytes("l"),Bytes.toBytes("mail"),Bytes.toBytes(mail));
//                    puts.add(mailPut);
//                }
//                if(trueName != null && trueName.length()>1){
//                    Put namePut  = new Put(Bytes.toBytes(TypeCode.RENREN_ID+"\u0000"+rrId));
//                    namePut.add(Bytes.toBytes("l"),Bytes.toBytes("trueName"),Bytes.toBytes(trueName));
//                    puts.add(namePut);
//                }
                if(idCard != null && idCard.length()>10){
                    Put idPut  = new Put(Bytes.toBytes(TypeCode.RENREN_ID+"\u0000"+rrId));
                    idPut.add(Bytes.toBytes("l"),Bytes.toBytes("idCard"),Bytes.toBytes(idCard));
                    puts.add(idPut);

                    count++;
                    idPut.setWriteToWAL(false);
                    if(count%10000 == 0) System.out.println("count  = "+count +" time used: "+(System.currentTimeMillis()-start)+" ms. "+idCard);
                    table.put(idPut);
                }

            }

//            if(value.length == 3){//rrid + mail + truename + idcard
//                count3++;
////                System.out.println("3--"+value[0]+value[1]+value[2]);
//                String rrId = value[0];
//                String mail = value[1];
//                String trueName = value[2];
//
//
//                if(mail != null && mail.contains("@")){
//                    Put mailPut  = new Put(Bytes.toBytes(TypeCode.RENREN_ID+"\u0000"+rrId));
//                    mailPut.add(Bytes.toBytes("l"),Bytes.toBytes("mail"),Bytes.toBytes(mail));
//                    puts.add(mailPut);
//                }
//                if(trueName != null && trueName.length()>1){
//                    Put namePut  = new Put(Bytes.toBytes(TypeCode.RENREN_ID+"\u0000"+rrId));
//                    namePut.add(Bytes.toBytes("l"),Bytes.toBytes("trueName"),Bytes.toBytes(trueName));
//                    puts.add(namePut);
//                }
//
//            }
//
//
//            if(value.length == 2){//rrid + mail + truename + idcard
//                count2++;
//                String rrId = value[0];
//                String mail = value[1];
//
//                if(mail != null && mail.contains("@")){
//                    Put mailPut  = new Put(Bytes.toBytes(TypeCode.RENREN_ID+"\u0000"+rrId));
//                    mailPut.add(Bytes.toBytes("l"),Bytes.toBytes("mail"),Bytes.toBytes(mail));
//                    puts.add(mailPut);
//                }
//
//            }


//            for(Put put:puts) {
//                count++;
//                put.setWriteToWAL(false);
//                if(count%10000 == 0) System.out.println("count  = "+count +" time used: "+(System.currentTimeMillis()-start)+" ms.");
//            }
//            table.put(puts);
        }

        table.flushCommits();
        table.close();
        System.out.println("count="+count+" count4="+count4+" count3="+count3+" count2="+count2);
    }


}
