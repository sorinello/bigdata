package bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		System.out.println(args[0]);
		System.out.println(args[1]);
		System.out.println(args[2]);
		Configuration conf = new Configuration();
		conf.set("IP", "178.154.179.250");
		Job job = new Job(conf, "wordcount");

		// FileSystem fs = FileSystem.get(conf);
		// fs.delete(new Path("/home/sorinello/hadoop/hdfs/bigdata-out"), true);
		// // delete file, true for recursive
		//
		// fs.close();
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setJarByClass(Map.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}

}
