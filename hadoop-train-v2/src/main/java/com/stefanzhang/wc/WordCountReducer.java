package com.stefanzhang.wc;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


/**
 * Input: (hello, 1)(world, 1)...
 *        (hello, 1)(world, 1)...
 *        (hello, 1)(world, 1)...
 *
 * Reducer1: (hello, 1)(hello, 1)(hello, 1)... ==> (hello, <1,1,1>)
 * Reducer2: (world, 1)(world, 1)(world, 1)... ==> (world, <1,1,1>)
 *
 * Output: (hello, 3)(world, 3) ...
 *
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();

        //<1,1,1>)
        while (iterator.hasNext()){
            IntWritable num = iterator.next();
            count += num.get();
        }
        context.write(key, new IntWritable(count));
    }
}
