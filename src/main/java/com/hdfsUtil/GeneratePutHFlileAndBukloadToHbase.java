package com.hdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * @author by Lx
 */
public class GeneratePutHFlileAndBukloadToHbase {
    public static class TextSplit extends Mapper<LongWritable, Text, Text, Text> {
        private Text wordTextOne = new Text();
        private Text wordTextTwo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String[] splits = line.split(",");
            //宽带业务号码
            wordTextOne.set(splits[0]);
            //手机号码:省份id
            wordTextTwo.set(splits[1]+":"+splits[2]);
            try {
                context.write(wordTextOne, wordTextTwo);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    public static class TextCombine extends Reducer<Text, Text, Text, Text> {
        private Text wordTextThree = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> valuelist, Context context) {
            // "宽带号码"====>"手机号码1:省份id 手机号码2：省份id2 ……"
            String tmp = "";
            for (Text text : valuelist) {
                tmp = tmp + text.toString() + " ";
            }
            wordTextThree.set(tmp);
            try {
                context.write(key, wordTextThree);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static class ConvertTexToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            //宽带业务号码\t手机号码1:省份id 手机号码2：省份id2 ……
            String[] items = value.toString().split("\t");
            //宽带业务号码
            String rowkeyWord = items[0];
            //手机号码1:省份id 手机号码2：省份id2 ……
            String valueWord = items[1];
            ///手机号码n:省份idn ……
            String[] valuelist = valueWord.split(" ");
            //宽带业务号码
            byte[] rowkey = Bytes.toBytes(rowkeyWord);
            byte[] family = Bytes.toBytes("phone");
            ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowkey);
            String pingColumn = "PH";
            for (int i = 0; i < valuelist.length; i++) {
                Put put = new Put(rowkey);
                byte[] column = Bytes.toBytes(pingColumn + i);
                byte[] valueIs = Bytes.toBytes(valuelist[i]);
                put.addColumn(family, column, valueIs);

                try {
                    context.write(immutableBytesWritable, put);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

        }
    }

    public static void main(String[] args) throws Exception {
        //第一个mapreduce
        Configuration configuration = new Configuration();
        //String[] dfsArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Job job = Job.getInstance(configuration);
        job.setJobName("Textsplit");
        job.setJarByClass(GeneratePutHFlileAndBukloadToHbase.class);
        job.setMapperClass(TextSplit.class);
        job.setReducerClass(TextCombine.class);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        //FileInputFormat.setInputPaths(job,new Path(dfsArgs[0]));
        //FileOutputFormat.setOutputPath(job,new Path(dfsArgs[1]));
//        FileInputFormat.setInputPaths(job, new Path("hdfs://beh:9000/bb2h"));
//        //FileOutputFormat.setOutputPath(job, new Path("hdfs://beh:9000/out/mid/"));
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.202.129:9000/prodata"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.202.129:9000/out/mid"));
        int result1 = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("First is " + result1);

        //第二个map
        Configuration hbaseconfiguration = HBaseConfiguration.create();
        hbaseconfiguration.set("HADOOP_USER_NAME","hadoop");
        //hbaseconfiguration.set("mapreduce.app-submission.cross-platform", "true");
        //hbaseconfiguration.set("mapreduce.framework.name", "yarn");
        Job jobToHfile = Job.getInstance(hbaseconfiguration);
        jobToHfile.setJobName("HFile");
        jobToHfile.setJarByClass(GeneratePutHFlileAndBukloadToHbase.class);
        jobToHfile.setMapperClass(ConvertTexToHFileMapper.class);
        jobToHfile.setMapOutputKeyClass(ImmutableBytesWritable.class);
        jobToHfile.setMapOutputValueClass(Put.class);
        //jobToHfile.setInputFormatClass(TextInputFormat.class);
        jobToHfile.setOutputFormatClass(HFileOutputFormat2.class);
//        FileInputFormat.setInputPaths(jobToHfile, new Path("hdfs://beh:9000/out/mid/"));
//        FileOutputFormat.setOutputPath(jobToHfile, new Path("hdfs://beh:9000/out/end/"));
        FileInputFormat.setInputPaths(jobToHfile, new Path("hdfs://192.168.202.129:9000/out/mid/"));
        FileOutputFormat.setOutputPath(jobToHfile, new Path("hdfs://192.168.202.129:9000/out/end/"));

        //System.setProperty("HADOOP_USER_NAME", "hadoop");
        Connection connection = null;
        //hbaseconfiguration.set("hadoop.user.name","hadoop");
        connection =  ConnectionFactory.createConnection(hbaseconfiguration);
        HTable htable = (HTable) connection.getTable(TableName.valueOf("broadbanduseridtophonenumber"));
        HFileOutputFormat2.configureIncrementalLoad(jobToHfile, htable, connection.getRegionLocator(TableName.valueOf("broadbanduseridtophonenumber")));
        int result2=jobToHfile.waitForCompletion(true) ? 0 : 1;
        System.out.println("Second is "+result2);
       // System.setProperty("HADOOP_USER_NAME", "hadoop");
        LoadIncrementalHFiles loder = new LoadIncrementalHFiles(hbaseconfiguration);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("broadbanduseridtophonenumber"));
        //loder.doBulkLoad(new Path("hdfs://beh:9000/out/end/"),htable);
        loder.doBulkLoad(new Path("hdfs://192.168.202.129:9000/out/end/"), admin, table, connection.getRegionLocator(TableName.valueOf("broadbanduseridtophonenumber")));
        System.out.println("END");
        System.exit(result2);


    }

}
