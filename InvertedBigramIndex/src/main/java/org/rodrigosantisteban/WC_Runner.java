package org.rodrigosantisteban;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WC_Runner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(WC_Runner.class);
        job.setJobName("InvertedIndex");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WC_Mapper.class);
        job.setReducerClass(WC_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // Configuración adicional para procesar grandes volúmenes de datos
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 10737418240L); // 10 GB
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", 107374182L); // 100 MB



        job.waitForCompletion(true);
    }
}
