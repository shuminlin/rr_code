package com.renren.bigdata;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-9
 * Time: 上午9:35
 * To change this template use File | Settings | File Templates.
 */
public class Separator {
    public static byte[] TYPE_SEP = Bytes.toBytes("/U0000");
    public static byte[] ID_SEP = Bytes.toBytes("/U0001");
}
