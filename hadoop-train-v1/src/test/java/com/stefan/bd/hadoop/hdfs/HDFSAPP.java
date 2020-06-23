package com.stefan.bd.hadoop.hdfs;

import com.google.gson.internal.$Gson$Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Use JAVA API Manipulate HDFS
 **/

public class HDFSAPP {

    public static final String HDFS_PATH = "hdfs://192.168.100.128:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUP() throws Exception{
        System.out.println("----------Setup----------");
        configuration = new Configuration();
        /**
         * Construct a HDFS Client Object
         * Param 1: URI
         * Param 2: Config
         * Param 3: User name
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    /**
     *
     * Make HDFS directory
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void creat() throws Exception{
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        out.writeUTF("hello there!");
        out.flush();
        out.close();
    }

    /**
     * Open HDFS content
     */
    @Test
    public void text() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("----------ShutDown----------");
    }


//    public static void main(String[] args) throws Exception{
//
//        Configuration configuration = new Configuration();
//
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://47.252.26.166:8020"), configuration, "hadoop");
//
//        Path path = new Path("/hdfsapi/test");
//
//        boolean result =  fileSystem.mkdirs(path);
//
//        System.out.println(result);
//    }

}
