package wc.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Split the phone#, up and down based on tab
 *  KEYIN: Offset of each line's starting position, LongWritable
 *  VALUEIN: lines of String, Text
 *
 *  KEYOUT: Map output key, Text
 *  VALUEOUT: Access Object, Access
 */

public class AccessMapper extends Mapper <LongWritable, Text, Text, Access>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Get stuff out, split

        String[] lines = value.toString().split("\t");
        String phone = lines[1];
        Long up = Long.parseLong(lines[lines.length-3]);
        Long down = Long.parseLong(lines[lines.length-2]);

        context.write(new Text(phone), new Access(phone, up, down));
    }

}
