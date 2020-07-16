package wc.access;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccesslocalAPP {


    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccesslocalAPP.class);

        job.setJarByClass(AccesslocalAPP.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // Set Partitioner
        job.setPartitionerClass(AccessPartitioner.class);
        job.setNumReduceTasks(3);

        // Mapper Output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);
        // Reducer Output
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path("access/input"));
        FileOutputFormat.setOutputPath(job, new Path("access/output"));

        job.waitForCompletion(true);
    }
}
