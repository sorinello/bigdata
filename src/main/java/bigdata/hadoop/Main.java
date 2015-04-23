package bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    private static Long TOTAL_BYTES = 0L;

    public static void main(String[] args) throws ClassNotFoundException,
        IOException, InterruptedException {

        callJobOne();
        callJobTwo();

    }

    private static void callJobOne() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration confOne = new Configuration();
        confOne.set("IP", "178.154.179.250");

        // confOne.set("IP", "192.168.0.1");
        Job jobOne = new Job(confOne, "jobOne");

        FileSystem fsOne = FileSystem.get(confOne);
        fsOne.delete(new Path("/home/sorinello/jobOneOutput"), true);
        // delete file, true for recursive
        fsOne.close();

        jobOne.setOutputKeyClass(LongWritable.class);
        jobOne.setOutputValueClass(LongWritable.class);

        jobOne.setJarByClass(FileMap.class);
        jobOne.setMapperClass(FileMap.class);
        jobOne.setReducerClass(FileReduce.class);

        jobOne.setInputFormatClass(TextInputFormat.class);
        jobOne.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobOne, new Path("/home/sorinello/hadoop/hdfs/bigdata"));
        FileOutputFormat.setOutputPath(jobOne, new Path("/home/sorinello/jobOneOutput"));

        jobOne.waitForCompletion(true);

        TOTAL_BYTES = jobOne.getCounters().getGroup("SORINELLO").findCounter("TOTAL BYTES").getValue();

        System.out.println("COUNTERSORINELLO: " + TOTAL_BYTES);
    }

    private static void callJobTwo() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration confTwo = new Configuration();
        confTwo.setLong("TOTAL_BYTES", TOTAL_BYTES);
        Job jobTwo = new Job(confTwo, "jobTwo");
        //
        FileSystem fsTwo = FileSystem.get(confTwo);
        fsTwo.delete(new Path("/home/sorinello/jobTwoOutput"), true);
        // delete file, true for recursive
        fsTwo.close();

        jobTwo.setOutputKeyClass(LongWritable.class);
        jobTwo.setOutputValueClass(LongWritable.class);

        jobTwo.setJarByClass(TotalsMap.class);
        jobTwo.setMapperClass(TotalsMap.class);
        jobTwo.setReducerClass(TotalsReduce.class);

        jobTwo.setInputFormatClass(TextInputFormat.class);
        jobTwo.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobTwo, new Path("/home/sorinello/jobOneOutput"));
        FileOutputFormat.setOutputPath(jobTwo, new Path("/home/sorinello/jobTwoOutput"));

        jobTwo.waitForCompletion(true);
    }

}
