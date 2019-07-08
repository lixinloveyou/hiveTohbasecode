package com.hdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
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
/**
 * @author by Lx
 */
public class GeneratePutHFlileAndBukloadToHbase {
    public static class TextSplit extends Mapper<LongWritable,Text,Text,Text>{
        private Text wordTextOne = new Text();
        private Text wordTextTwo = new Text();
        @Override
        protected void  map(LongWritable key, Text value, Context context){
            String line = value.toString();
            String [] splits = line.split(",");
                wordTextOne.set(splits[0]);
                wordTextTwo.set(splits[1]);
                try {
                    context.write(wordTextOne,wordTextTwo);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

        }

    }
    public static class  TextCombine extends Reducer<Text,Text,Text,Text>{
        private  Text wordTextThree = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> valuelist,Context context){
            String tmp="";
            for (Text text: valuelist){
                    tmp = tmp+text.toString()+" ";
                }
                wordTextThree.set(tmp);
            try {
                context.write(key,wordTextThree);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    public static  class  ConvertTexToHFileMapper extends  Mapper<LongWritable,Text, ImmutableBytesWritable, Put>{
        @Override
        protected void  map(LongWritable key, Text value, Context context){
            String [] items = value.toString().split("\t");
            String  rowkeyWord = items[0];
            String  valueWord =items[1];
            String [] valuelist  = valueWord.split(" ");
            byte [] rowkey = Bytes.toBytes(rowkeyWord);
            byte [] family=Bytes.toBytes("brand2phone");
            ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowkey);
            String pingColumn ="BD";
            for (int i =0;i<valuelist.length;i++){
                Put put =new Put(rowkey);
                byte[] column= Bytes.toBytes(pingColumn+i);
                byte[] valueIs = Bytes.toBytes(valuelist[i]);
                put.addColumn(family,column,valueIs);

                try {
                    context.write(immutableBytesWritable,put);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

     }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration =new Configuration();
        String [] dfsArgs= new GenericOptionsParser(configuration,args).getRemainingArgs();
        Job job = Job.getInstance(configuration);
        job.setJobName("Textsplit");
        job.setJarByClass(GeneratePutHFlileAndBukloadToHbase.class);
        job.setMapperClass(TextSplit.class);
        job.setReducerClass(TextCombine.class);
        System.out.println(dfsArgs[0]+" "+dfsArgs[1]+" "+dfsArgs[2]);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(dfsArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(dfsArgs[1]));

        int result1 = job.waitForCompletion(true)?0:1;

        System.out.println("this is "+ result1);
        Configuration hbaseconfiguration = HBaseConfiguration.create();
        Job jobToHfile =Job.getInstance(hbaseconfiguration);
        jobToHfile.setJarByClass(GeneratePutHFlileAndBukloadToHbase.class);
        jobToHfile.setMapperClass(ConvertTexToHFileMapper.class);
        jobToHfile.setMapOutputKeyClass(ImmutableBytesWritable.class);
        jobToHfile.setMapOutputValueClass(Put.class);
        jobToHfile.setOutputFormatClass(HFileOutputFormat2.class);




        //final String HBASE_ZOOKEEPER_QUORUM="hbase.zookeeper.quorum";

        //hbaseconfiguration.set(HBASE_ZOOKEEPER_QUORUM,"192.168.202.130:2181,192.168.202.129:2181,192.168.202.131:2181");
        Connection connection =null;
        connection= ConnectionFactory.createConnection(hbaseconfiguration);

        HTable htable =  (HTable) connection.getTable(TableName.valueOf("brandboard"));
        HFileOutputFormat2.configureIncrementalLoad(jobToHfile,htable,connection.getRegionLocator(TableName.valueOf("brandboard")));

        FileInputFormat.setInputPaths(jobToHfile,new Path(dfsArgs[1]));
        FileOutputFormat.setOutputPath(jobToHfile,new Path(dfsArgs[2]));

        System.out.println(jobToHfile.waitForCompletion(true)?0:1);

        LoadIncrementalHFiles loder = new LoadIncrementalHFiles(hbaseconfiguration);
        Admin admin = connection.getAdmin();
        Table table=connection.getTable(TableName.valueOf("brandboard"));

        int result2 = jobToHfile.waitForCompletion(true)?0:1;
        System.out.println("that is "+ result2);


        loder.doBulkLoad(new Path(dfsArgs[2]),admin,table,connection.getRegionLocator(TableName.valueOf("brandboard")));
        System.exit(result2);



    }

}
