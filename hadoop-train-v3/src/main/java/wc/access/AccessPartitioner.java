package wc.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner based on the first two digits
 */

public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text phone, Access access, int numPartitions) {

        if (phone.toString().startsWith("13")){
            return 0;
        }
        else if(phone.toString().startsWith("15")){
            return 1;
        }
        else {
            return 2;
        }
    }
}
