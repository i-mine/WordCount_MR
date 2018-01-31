package com.mobvista.dataplatform;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 *date: 2018-01-23
 *author lei.du
 *desc: Mapreduce WordCount
 */
public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            Job job = Job.getInstance(getConf(),"MR_Test-_Wordcount-lei.du");//新建一个Job
            job.setJarByClass(Main.class);
            job.setMapOutputKeyClass(Text.class);//map的输出key
            job.setMapOutputValueClass(IntWritable.class);//reduce的输出value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(WCMapper.class);//设置map类
            job.setReducerClass(WCReducer.class);//设置reduce类

            job.setNumReduceTasks(2);//设置reduce的个数

            job.setCombinerClass(WCReducer.class);//设置combiner类
            job.setPartitionerClass(MyPartion.class);//设置自定义分区类
            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileInputFormat.setInputPaths(job,in);
            FileOutputFormat.setOutputPath(job,out);
            System.exit(job.waitForCompletion(true)?0:1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
class WCMapper extends Mapper<Writable,Text,Text,IntWritable>{
    private IntWritable one  = new IntWritable(1);
    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        String [] words = value.toString().split(" ");
        for(String word:words){
            if(word.length()>10){
                context.getCounter("CUSTOM_BAD_WORDS_COUNTER","BAD_WORDS_COUNTS").increment(1);
            }
            context.write(new Text(word),one);
        }
    }
}
class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value:values){
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
class MyPartion extends Partitioner<Text, IntWritable>{
    @Override
    public int getPartition(Text key, IntWritable intWritable, int reducenum) {
        String k = key.toString();
        if(k.length()%2==0){
            return 0;
        }else {
            return 1;
        }
    }
}