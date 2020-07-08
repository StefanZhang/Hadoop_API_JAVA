package com.stefan.bd.hadoop.hdfs;

/**
 * wordcount class
 */

public class WordCountMapper implements Mapper{

    public void map(String line, Context context) {
        String[] words = line.split("\t"); //.toLowerCase()
        for (String word : words){
            Object value = context.get(word);
            if(value == null){
                // If a word is never found
                context.write(word, 1);
            }
            else{
                // Else increment by 1
                int val = Integer.parseInt(value.toString());
                context.write(word, val+1);
            }
        }
    }
}
