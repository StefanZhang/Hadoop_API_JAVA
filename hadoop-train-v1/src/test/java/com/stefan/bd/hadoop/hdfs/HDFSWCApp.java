package com.stefan.bd.hadoop.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Use HDFS API to implement WordCount
 *
 * Input: a file from HDFS
 * Output: a file contain the results to HDFS
 *
 * Steps:
 * 1. Read file from HDFS ==> HDFS API
 * 2. Word count ==> Mapper
 * 3. Cache the results ==> Context
 * 4. Output the results to HDFS ==> HDFS API
 */

public class HDFSWCApp {

    public static final String HDFS_PATH = "hdfs://192.168.100.128:8020";

    public static void main(String[] args) throws Exception{

        //1. Read file from HDFS ==> HDFS API
        Path input = new Path("/hdfsapi/test/wc.txt");

        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration(), "root");

        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input, false);

        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream instream = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(instream));

            String line = "";
            while ((line = reader.readLine()) != null){
                //TODO 2. Word count ==> Mapper

            }

            reader.close();
            instream.close();

        }

        //TODO 3. Cache the results ==> Context
        Map<Object, Object> contextMap = new HashMap<Object, Object>();

        //4. Output the results to HDFS ==> HDFS API
        Path output = new Path("/hdfsapi/test/");

        FSDataOutputStream outstream = fileSystem.create(new Path(output, new Path("wc.out")));

        //TODO Put results from step3(cache) to output stream
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();

        for(Map.Entry<Object, Object> entry : entries){
            outstream.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }

        outstream.close();
        fileSystem.close();

        System.out.println("HDFS WC complete!");
    }


}
