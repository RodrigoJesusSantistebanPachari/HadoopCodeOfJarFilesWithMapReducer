package org.rodrigosantisteban;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WC_Reducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> documentCounts = new HashMap<>();

        for (Text value : values) {
            String documentId = value.toString();
            String[] parts = documentId.split(":", 2);

            if (parts.length == 2) {
                String docId = parts[0];
                int count = Integer.parseInt(parts[1]);

                documentCounts.put(docId, documentCounts.getOrDefault(docId, 0) + count);
            }
        }

        StringBuilder documentList = new StringBuilder();

        for (Map.Entry<String, Integer> entry : documentCounts.entrySet()) {
            String documentId = entry.getKey();
            int count = entry.getValue();
            documentList.append(documentId).append(":").append(count).append(" ");
        }

        context.write(key, new Text(documentList.toString().trim()));
    }
}
