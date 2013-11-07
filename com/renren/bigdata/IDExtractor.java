package com.renren.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SecurityUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-9
 * Time: 上午10:57
 * To change this template use File | Settings | File Templates.
 */
public class IDExtractor {
    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/t_user_info.csv";
        BufferedWriter qqWriter  = new BufferedWriter(new FileWriter("/Users/zyshen/rrid_qq"));
        BufferedWriter mobileWriter  = new BufferedWriter(new FileWriter("/Users/zyshen/rrid_mobile"));
        BufferedReader reader =new BufferedReader(new FileReader(INPUT_FILE));


        String line = null;

        int count  = 0;
        int errorCount = 0;
        int notDig = 0;
        int notMailCount = 0;
        int qqMailCount = 0;
        int qqCount = 0;
        int mobileMailCount = 0;
        int mobileCount= 0;

        long start  = System.currentTimeMillis();
        while ((line = reader.readLine()) != null) {
            count++;
            String[] value = line.split("\\,");
            if(value.length<2) {
                errorCount++;
                System.out.println("Error: "+value);
                continue;
            }
            String id = value[0];
            String mail = value[1];


            if(!IDCheck.isDig(id)){
                System.out.println("bad id: "+id);
                notDig++;
            }
            if(!IDCheck.isMail(mail)){
//                System.out.println("bad mail: "+mail);
                notMailCount++;
            }
            if(IDCheck.isQQMail(mail)){
                qqMailCount++;
                String[] tmp = mail.split("\\@");
                String qq = tmp[0];
                if(IDCheck.isDig(qq)){
                    qqCount++;
                    qqWriter.write(id+"\t"+qq+"\n");
                }
            }
            if(IDCheck.isMobileMail(mail)){
                mobileMailCount++;
                String[] tmp = mail.split("\\@");
                String mobile  = tmp[0];
                if(IDCheck.isMobile(mobile)){
                    mobileCount++;
                    mobileWriter.write(id+"\t"+mobile+"\n");
                }

            }

        }
        qqWriter.close();
        mobileWriter.close();

        System.out.println("\n------------Summary---------------");
        System.out.println("Total = "+count);
        System.out.println("Error = "+errorCount);
        System.out.println("Bad Mail = "+notMailCount);
        System.out.println("Not Digital = "+notDig);

        System.out.println("\nQQ Mail Count = "+qqMailCount);
        System.out.println("QQ Count = "+qqCount);
        System.out.println("\nMobile Mail Count = "+mobileMailCount);
        System.out.println("Mobile Count = "+mobileCount);

        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }

}
