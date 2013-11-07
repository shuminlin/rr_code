package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SecurityUtil;
import java.security.MessageDigest;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-12
 * Time: 下午3:11
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUtil {

    private static boolean CheckPrivacy = false;

    public static byte[] compositeKey(String type1, String id1, String type2, String id2){
        byte[] s1;
        s1 = Bytes.add(Bytes.toBytes(type1), Bytes.toBytes("\u0000"), Bytes.toBytes(id1));
        s1 = Bytes.add(s1,Bytes.toBytes("\u0001"));
        s1 = Bytes.add(s1,Bytes.toBytes(type2),Bytes.toBytes("\u0000"));
        s1 = Bytes.add(s1,Bytes.toBytes(id2));

        return s1;
    }

    public static List<String> getNeighbor(String id, Configuration conf, String tableName) throws Exception{
        List<String> neighbors = new ArrayList<String>();
        HTable table = new HTable(conf,tableName);
        int count =0;

        byte[] startKey = Bytes.toBytes(id);

        byte[] endKey = Bytes.toBytes(Bytes.toString(startKey)+"/u0001");

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);

        ResultScanner scanner = table.getScanner(scan);

        for(Result result:scanner){
//            System.out.println(Bytes.toString(result.getRow())+" "+result.toString());
            String neighbor =  Bytes.toString(result.getRow()).split("\u0001")[1];
            neighbors.add(neighbor);
            count++;
        }

//        System.out.println("Start key = "+Bytes.toString(startKey)+"\nEnd key = "+Bytes.toString(endKey)+"\nTotal = "+count);
//        System.out.println("\nTotal = "+count);

        return neighbors;

    }

    public static void dumpId(List<String> ids){
        for(String id:ids){
           String type  = id.split("\u0000")[0];
           String real_id  = id.split("\u0000")[1];
           String outputFlag ="default flag";
           if(TypeCode.MAIL.equals(type)){
                outputFlag = "Mail Box";
           }
           if(TypeCode.RENREN_ID.equals(type)){
                outputFlag = "RenRen ID";
           }
           if(TypeCode.ID.equals(type)){
                outputFlag = "Civil Card ID";
           }
           if(TypeCode.IOS_UUID1.equals(type)){
                outputFlag = "IOS UUID";
           }
            if(TypeCode.QQ.equals(type)){
                outputFlag = "QQ";
            }
            if(TypeCode.MOBILE.equals(type)){
                outputFlag = "Mobile";
            }

            System.out.println("[ID_HUB] "+outputFlag+": "+real_id);

        }
        System.out.println("ID_HUB Total = "+ids.size());
    }

    public static void dumpLabel(List<String> ids, Configuration conf, String tableName) throws Exception{

        HTable table = new HTable(conf,tableName);
        int count =0;

        for (String id:ids){

            byte[] startKey = Bytes.toBytes(id);
            byte[] endKey = Bytes.toBytes(Bytes.toString(startKey)+"\u0001");

            Scan scan = new Scan();
            scan.setStartRow(startKey);
            scan.setStopRow(endKey);

            ResultScanner scanner = table.getScanner(scan);

            for(Result result:scanner){
                String rowKey =  Bytes.toString(result.getRow());
                List<KeyValue> kvs = result.list();
//                System.out.println("---Labels from "+rowKey);
                for(KeyValue kv:kvs){
                    String label = Bytes.toString(kv.getQualifier());


                    String value = Bytes.toString(kv.getValue());
                    if(checkPrivacy(label)&&CheckPrivacy) {
//                        value = value.hashCode()+"";
                        MessageDigest md = MessageDigest.getInstance("MD5");
//                        value = Bytes.toString(md.digest(Bytes.toBytes(value)));
                        value = md.digest(Bytes.toBytes(value)).toString();

                    }

                    if(label.startsWith("interest.")){
                        label = label.substring(10);
                        label = InterestCode.getInterest(new Integer(label).intValue());
                        label = "兴趣:"+label;
                    }
                    if(label.startsWith(" ")) continue;

                    if(label.startsWith("gender.female")) label = "女性概率";
                    if(label.startsWith("gender.male")) label = "男性概率";
                    if(label.equalsIgnoreCase("mail")) label = "邮箱";
                    if(label.equalsIgnoreCase("trueName")) label = "真名";
                    System.out.println("[Label] " + label +" = "+value);
                }
                count++;
            }
            scanner.close();

        }
        table.close();

    }

    public static boolean checkPrivacy(String label) {
        boolean result = false;
        String[] privacyTokens = new String[]{"trueName","idCard","mail"};
        for (String token:privacyTokens){
            if(label.equalsIgnoreCase(token)) return true;
        }

        return result;  //To change body of created methods use File | Settings | File Templates.
    }

    public static void dumpActivity(List<String> ids, Configuration conf, String tableName) throws Exception{

        HTable table = new HTable(conf,tableName);
        int count =0;

        for (String id:ids){

            byte[] startKey = Bytes.toBytes(id);
            byte[] endKey = Bytes.toBytes(Bytes.toString(startKey)+"/u0001");

            Scan scan = new Scan();
            scan.setStartRow(startKey);
            scan.setStopRow(endKey);

            ResultScanner scanner = table.getScanner(scan);
            System.out.println("\n[Activity Tracing] ID="+id);

//            if(scanner.next() == null) {System.out.print(" N/A at present!"); continue;  }
            int actCount = 0;
            for(Result result:scanner){
                String rowKey =  Bytes.toString(result.getRow());
                String time = rowKey.split("\u0001")[1];
                List<KeyValue> kvs = result.list();
                System.out.print("\n[Time:" + time.substring(0, 8) + "|" + time.substring(8) + "]");

                for(KeyValue kv:kvs){
                    actCount++;
                    String label = Bytes.toString(kv.getQualifier());
                    if(label.equalsIgnoreCase("visit")) label = "Visit_App";
                    if(label.equalsIgnoreCase("click")) label = "Click_AD";

                    System.out.print("<" + label + ":" + Bytes.toString(kv.getValue()) + ">");
                }

                count++;
            }
            if(actCount == 0) System.out.print(" N/A at present!");

            scanner.close();

        }
        table.close();
        System.out.println("\n\nActivity Details Total: "+count);

    }

    public static void dumpActSummary(List<String> ids, Configuration conf, String tableName) throws Exception{

        HTable table = new HTable(conf,tableName);
        int count =0;
        SortedMap<String,Integer> acts = new TreeMap<String, Integer>();

        for (String id:ids){

            byte[] startKey = Bytes.toBytes(id);
            byte[] endKey = Bytes.toBytes(Bytes.toString(startKey)+"/u0001");

            Scan scan = new Scan();
            scan.setStartRow(startKey);
            scan.setStopRow(endKey);

            ResultScanner scanner = table.getScanner(scan);

            for(Result result:scanner){
                String rowKey =  Bytes.toString(result.getRow());
                String time = rowKey.split("\u0001")[1];
                List<KeyValue> kvs = result.list();
//                System.out.println("[Activity Tracing] ID="+id +"  Time=" +time);

                for(KeyValue kv:kvs){
//                    System.out.println("<Activity> =" + Bytes.toString(kv.getQualifier()) +" <Target>="+Bytes.toString(kv.getValue()));
                    String action = Bytes.toString(kv.getQualifier());
                    String target = Bytes.toString(kv.getValue());

                    if(action.equalsIgnoreCase("visit")) action = "Visit_App";
                    if(action.equalsIgnoreCase("click")) action = "Click_AD";

                    String key = "["+action+"]"+":"+target;
                    if(acts.containsKey(key)){
                        Integer value  =  new Integer(acts.get(key).intValue()+1);
//                        System.out.println(key+ " :"+value.intValue());
                        acts.put(key,value);

                    }else{

                        acts.put(key,new Integer(1));

                    };
                }
                count++;
            }
            scanner.close();

        }
        table.close();
        for(Iterator iter = acts.entrySet().iterator();iter.hasNext();){
            Map.Entry kvs = (Map.Entry)iter.next();
            String key = kvs.getKey().toString();
            String value = kvs.getValue().toString();

            System.out.println(key+" ["+value+"]times.");
        }
//        System.out.println("Activity Total: "+count);

    }

    public static Put[] key2puts(String id1, String id2){
        Put[] putArray =  new Put[2];
        putArray[0] = new Put(Bytes.toBytes(id1+"\u0001"+id2));
        putArray[1] = new Put(Bytes.toBytes(id2+"\u0001"+id1));
        return putArray;
    }

    public static void main(String[] args) throws Exception{
        //start init
        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        Configuration conf = HBaseConfiguration.create();

        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");

        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");
        //init end

        List<String> ids;
        ids = getNeighbor(TypeCode.RENREN_ID+"\u0000"+"465197220",conf,"test_id_hub");
        for(String id:ids){
            System.out.println("matched id = "+id);
        }

    }
}
