package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
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
public class ImportUserInterest {



    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/t_user_interest_1000";
        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        HTable table = new HTable(conf,"test_user_label");
        table.setAutoFlush(false);


        long start = System.currentTimeMillis();

        String line = null;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            List<Put> puts = new ArrayList<Put>();
            String[] value = line.split("\\t");

            if(value.length == 2){//rrid + mail + civil id
                String uuid1 =  value[0];
                String label = value[1].split(":")[0];
                String weight = value[1].split(":")[1];
                int i = new Integer(label).intValue();
                System.out.println("label="+"'"+label+"'"+"|value="+value+"|"+i);
            //construct

//                Put put =  new Put(Bytes.toBytes(TypeCode.IOS_UUID1+"\u0000"+uuid1));
//                put.add(Bytes.toBytes("l"),Bytes.toBytes("interest."+label),Bytes.toBytes(weight));
//                put.setWriteToWAL(false);
//                Delete del = new Delete(Bytes.toBytes(TypeCode.IOS_UUID1+"\u0000"+uuid1));
//                del.deleteColumn(Bytes.toBytes("l"),Bytes.toBytes(label));
//                del.setWriteToWAL(false);

//                table.put(put);
//                table.delete(del);

                count++;
                if(count%10000 == 0) System.out.println("count  = "+count +" time used: "+(System.currentTimeMillis()-start)+" ms.");

            }

            if(value.length!=2){
                System.err.println("bad data encountered!! "+line);
            }


        }
        table.flushCommits();
        table.close();

        System.out.println("\n------------Summary---------------");
        System.out.println("Total = "+count);
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }

}
