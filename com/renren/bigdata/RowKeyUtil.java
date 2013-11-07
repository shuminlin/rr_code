package com.renren.bigdata;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-9
 * Time: 上午9:54
 * To change this template use File | Settings | File Templates.
 */
public class RowKeyUtil {
    public static byte[] getID2ID(String id1, String id2) throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(byteOutput);
        data.writeUTF(id1);
        data.write(Separator.ID_SEP);
        data.writeUTF(id2);

        return byteOutput.toByteArray();

    }
    public static byte[] getID(int type, String id) throws IOException{
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(byteOutput);
        data.write(Bytes.toBytes(type));
        data.write(Separator.TYPE_SEP);
        data.writeUTF(id);

        return byteOutput.toByteArray();
    }
}
