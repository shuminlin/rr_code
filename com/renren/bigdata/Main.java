package com.renren.bigdata;

import org.apache.hadoop.hbase.util.Bytes;
import static java.lang.System.out;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Main {

    public static void main(String[] args) throws NoSuchAlgorithmException {
	// write your code here
//    out.println(Bytes.toString(MessageDigest.getInstance("MD5").digest(Bytes.toBytes("123"))));
     String a = "74nini";
//        out.println(IDCheck.isDig(a));

        System.out.println((System.currentTimeMillis()/1000/60/60)%24);
    }
}
