package com.stefan.bd.hadoop.hdfs;

import com.google.gson.internal.$Gson$Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
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

        // Set the dfs.replication to 1, instead of hdfs-default as 3
        configuration.set("dfs.replication", "1");

        /**
         * Construct a HDFS Client Object
         * Param 1: URI
         * Param 2: Config
         * Param 3: User name
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    /**
     * Make HDFS directory
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * Make new file a.txt with string
     */

    @Test
    public void creat() throws Exception{
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
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

    /**
     * Rename files
     * */

    @Test
    public void rename() throws Exception {
        Path orgPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/c.txt");
        boolean r = fileSystem.rename(orgPath, newPath);
        System.out.println(r);
    }

    /***
     * Copy local files to HDFS
     */
    @Test
    public void CopyFromLocal() throws Exception{
        Path orgPath = new Path("C:\\Users\\Stefan\\OneDrive\\桌面\\local.txt");
        Path newPath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(orgPath, newPath);
    }

    /***
     * Copy local large files to HDFS, with progress
     */
    @Test
    public void CopyFromLocalLarge() throws Exception{

        Path newPath = new Path("/hdfsapi/test/Aladdin.mkv");

        // Define input stream
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:\\Aladdin.mkv")));

        FSDataOutputStream out = fileSystem.create(newPath, new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
            });

        IOUtils.copyBytes(in, out, 4096);

    }

    /**
     * List the files in dfs
     */
    @Test
    public void list() throws Exception{
        FileStatus[] status = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus stat : status){
            String isDir = stat.isDirectory() ? "Dir" : "File";
            String permission = stat.getPermission().toString();
            short rep = stat.getReplication();
            long len = stat.getLen();
            String path = stat.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + rep + "\t" + len + "\t" + path);
        }

    }

    /**
     * Delete a file
     */
    @Test
    public void delete() throws Exception{
        boolean del = fileSystem.delete(new Path("/hdfsapi/test/Aladdin.mkv"), true);
        System.out.println(del);
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
