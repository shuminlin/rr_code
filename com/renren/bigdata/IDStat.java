package com.renren.bigdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-9
 * Time: 上午10:57
 * To change this template use File | Settings | File Templates.
 */
public class IDStat {
    public static void main(String[] args) throws Exception{
        String INPUT_FILE = "/Users/zyshen/t_user_info.csv";
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
        int fnEduCount = 0;
        int cnEduCount = 0;

        HashSet<String> cnEduSet = new HashSet<String>();
        HashSet<String> fnEduSet = new HashSet<String>();

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
                }
            }
            if(IDCheck.isMobileMail(mail)){
                mobileMailCount++;
                String[] tmp = mail.split("\\@");
                String mobile  = tmp[0];
                if(IDCheck.isMobile(mobile)){
                    mobileCount++;
                }

            }

            if(mail.contains(".edu")&&(!mail.contains("edu.cn"))){
                fnEduCount++;
                fnEduSet.add(mail.split("@")[1]);
            }
            if(mail.contains(".edu.cn")){
                cnEduCount++;
                cnEduSet.add(mail.split("@")[1]);
            }


        }

        System.out.println("\n------------Summary---------------");
        System.out.println("Total = "+count);
        System.out.println("Error = "+errorCount);
        System.out.println("Bad Mail = "+notMailCount);
        System.out.println("Not Digital = "+notDig);

        System.out.println("\nQQ Mail Count = "+qqMailCount);
        System.out.println("QQ Count = "+qqCount);
        System.out.println("\nMobile Mail Count = "+mobileMailCount);
        System.out.println("Mobile Count = "+mobileCount);

        System.out.println("\nfnEdu Count = "+fnEduCount +" Unique = "+fnEduSet.size());
        System.out.println("cnEdu Count = "+cnEduCount+" Unique = "+cnEduSet.size());

        ////////////////dump data
        BufferedWriter cnWriter  = new BufferedWriter(new FileWriter("/Users/zyshen/cn_edu"));
        BufferedWriter fnWriter  = new BufferedWriter(new FileWriter("/Users/zyshen/fn_edu"));
        for(String val:cnEduSet){
            cnWriter.write(val+"\n");
        }
        for(String val:fnEduSet){
            fnWriter.write(val+"\n");
        }
        cnWriter.close();
        fnWriter.close();


        System.out.println("\nAccomplished! \nTime escaped "+(System.currentTimeMillis()-start) + " ms.");
    }

}
