package com.stefanzhang.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kerby.config.Conf;

import java.io.FileOutputStream;

/**
 * Driver, sets up the parameters
 *
 * Test on Local
 */

public class WordCountApp {

    public static void main(String[] args) throws Exception{

        //TODO..
        //Windows setting only
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-3.2.1");
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.100.128:8020");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountApp.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // Mapper Output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Reducer Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Input and Output path
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        // Send job
        boolean result = job.waitForCompletion(true);

        if (result){
            System.out.println("Successful");
            System.exit(0);
        }
        else {
            System.exit(1);
        }
    }

}
