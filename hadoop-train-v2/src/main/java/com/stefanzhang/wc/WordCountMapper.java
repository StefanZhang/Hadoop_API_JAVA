package com.stefanzhang.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Offset of each line's starting position, LongWritable
 * VALUEIN: lines of String, Text
 *
 * KEYOUT: Map output key, Text
 * VALUEOUT: Map output value, IntWritable
 *
 * (hello, 5) (world, 3)...
 *
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Split the value based on the "\t"
        String[] words = value.toString().split("\t");

        for (String word : words){
            context.write(new Text(word), new IntWritable(1));
        }

    }


    
}
