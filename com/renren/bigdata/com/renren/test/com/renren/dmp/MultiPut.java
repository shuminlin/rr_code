package com.renren.bigdata.com.renren.test.com.renren.dmp;

import com.renren.bigdata.TypeCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.security.SecurityUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-18
 * Time: 下午4:01
 * To change this template use File | Settings | File Templates.
 */
public class MultiPut {
    public static void main(String[] args)throws Exception{
        String fileName = "/Users/zyshen/rrid_qq";
        int THREAD_AMOUNT = 100;
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000000);
        ExecutorService service = Executors.newCachedThreadPool();
        PutThread[] c = new PutThread[THREAD_AMOUNT];

        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");
        Configuration conf = HBaseConfiguration.create();
        conf.set("/Users/zyshen/work/hbase.keytab","/Users/zyshen/work/hbase.keytab");
        conf.set("hbase@H.DATA.GAME.OPI.COM","hbase@H.DATA.GAME.OPI.COM");
        SecurityUtil.login(conf, "/Users/zyshen/work/hbase.keytab", "hbase@H.DATA.GAME.OPI.COM");

        String tableName = "poc_id_hub";
        HTablePool pool = new HTablePool(conf,100);
        HTableInterface[] tables = new HTableInterface[100];
        for(int i=0;i<100;i++){
            tables[i] = pool.getTable(tableName);
            tables[i].setAutoFlush(false);
//            tables[i].setWriteBufferSize(4*1024*1024);
        }
        for(int i=0;i<100;i++){
            pool.putTable(tables[i]);
        }
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_AMOUNT);



        long start = System.currentTimeMillis();

        for(int i=0;i<THREAD_AMOUNT;i++){
            c[i] = new PutThread(countDownLatch,queue,pool);
            new Thread(c[i]).start();

        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
//            int tmp  = 0;
            while ((line = reader.readLine())!=null){
                String rrid = line.split("\t")[0];
                String qq = line.split("\t")[1];
                line = TypeCode.RENREN_ID+"\u0000"+rrid+"\u0001"+TypeCode.QQ+"\u0000"+qq;
                queue.put(line);

                System.out.println("qq="+qq+" rrid="+rrid+" line="+line);

//                if (tmp >=100) System.exit(0);

            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        for(int i=0;i<THREAD_AMOUNT;i++){
            c[i].notifyProcessOver();
        }

        System.out.println("File read finished!");
        countDownLatch.await();
        pool.close();
        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");


    }

}
