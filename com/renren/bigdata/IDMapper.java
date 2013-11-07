package com.renren.bigdata;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.*;

public class IDMapper {

    public static int MAX_FINGER_PRINT = 1024;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int count = 0;
            String[] line = value.toString().split("\t");
            String uuid = line[0];
            String hour = line[1];
            String ip = line[2];

            String sig = hour+":"+ip;

            output.collect(new Text(uuid),new Text(sig));


        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int sum = 1;
            String sig;
            StringBuilder sb =  new StringBuilder();
            sig = values.next().toString();
            if(sig==""||sig.isEmpty()) return;
            
            sb.append(sig);
            while (values.hasNext()) {
                sum++;
                sig = values.next().toString();
                if(sum>MAX_FINGER_PRINT){
                    System.err.println("Error UUID Encountered!! uuid = "+key.toString());
                    continue;
                }
                if(sig == "") {
                    System.err.println("Error UUID Encountered!! Null Signature!!");
                    continue;
                }
                sb.append("|");
                sb.append(sig);
            }

//            if(sum >=4 )   output.collect(key, new Text(sum+sb.toString()));
//            if(sum>=4) reporter.incrCounter("abc","def",1);

            String sigs = sb.toString();

            output.collect(key, new Text(sigs));
        }
    }

    public static void main(String[] args) throws Exception {
        String INPUT = "/home/bigdata/project/unicity/201308";
        String OUTPUT = "/home/bigdata/project/unicity/201308_out6";
        if(args.length==2){
            INPUT = args[0];
            OUTPUT = args[1];
        }

//        System.setProperty("java.security.krb5.conf", "/Users/zyshen/work/env/krb5.h.conf");
//        System.setProperty("java.security.auth.login.config", "/Users/zyshen/work/hbase/conf/jaas.conf");

        JobConf conf = new JobConf(IDMapper.class);
//        conf.set("/Users/zyshen/work/env/bigdata.keytab","/Users/zyshen/work/env/bigdata.keytab");
//        conf.set("bigdata@H.DATA.GAME.OPI.COM","bigdata@H.DATA.GAME.OPI.COM");

//        SecurityUtil.login(conf, "/Users/zyshen/work/env/bigdata.keytab", "bigdata@H.DATA.GAME.OPI.COM");


        conf.setJobName("ID Mapper");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(32);


//
        FileInputFormat.setInputPaths(conf, new Path(INPUT));
        FileOutputFormat.setOutputPath(conf, new Path(OUTPUT));

        JobClient.runJob(conf);
    }
}
