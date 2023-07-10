package org.rodrigosantisteban;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WC_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text wordPair = new Text();
    private Text documentId = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t", 2);

        if (parts.length == 2) {
            String documentId = parts[0];
            String document = parts[1];
            String[] words = document.split("\\s+");

            if (words.length > 1) {
                for (int i = 0; i < words.length - 1; i++) {
                    wordPair.set(words[i] + " " + words[i + 1]);
                    context.write(wordPair, new Text(documentId + ":1"));
                }
            }
        }
    }
}
