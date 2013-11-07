package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SecurityUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-9
 * Time: 上午10:37
 * To change this template use File | Settings | File Templates.
 */
public class VerifyData {
    public static void main(String[] args) throws Exception{

        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");
        String tableName = "poc_id_hub";

        String type =  TypeCode.QQ;
        String id = "363342387";

//        String type =  TypeCode.RENREN_ID;
//        String id = "18701";



        byte[] startKey = Bytes.toBytes(type+"\u0000"+id);
        byte[] endKey = Bytes.toBytes(Bytes.toString(startKey)+"\u0002");

        HTable table = new HTable(conf,tableName);
        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);

        ResultScanner scanner = table.getScanner(scan);

        long start = System.currentTimeMillis();
        int count =0;
        List<String> idList = new ArrayList<String>();
        idList.add(Bytes.toString(startKey));
        for(Result result:scanner){
//            System.out.println(Bytes.toString(result.getRow())+" "+result.toString());
            String[] v =  Bytes.toString(result.getRow()).split("\u0001");
//            idList.add(v[0]);
            if(v[0].equals(v[1])) continue;
            idList.add(v[1]);
            count++;
        }
        System.out.println("\nQuery: "+"  type = "+TypeCode.class.getFields()[new Integer(type).intValue()-1].getName()+"      ID = "+id);


        System.out.println("\n-----------------------ID_Hub Summary----------------------");
        HBaseUtil.dumpId(idList);

        System.out.println("\n--------------------User Label Summary---------------------");
        HBaseUtil.dumpLabel(idList, conf,"test_user_label");

        System.out.println("\n-------------------User Behaviour Summary------------------");
        HBaseUtil.dumpActSummary(idList, conf,"test_user_activity");
        //        HBaseUtil.dumpActivity(idList, conf,"test_user_activity");

        System.out.println("\n--------------------User Activity Tracing------------------");
        HBaseUtil.dumpActivity(idList, conf,"test_user_activity");

//        System.out.println("\n------------Performance Summary---------------\nStart key = "+Bytes.toString(startKey)+"\nEnd key = "+Bytes.toString(endKey)+"\nTotal = "+count);
        System.out.println("\n--------------------------Summary--------------------------");
        System.out.println("Total = "+idList.size());
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }
}
