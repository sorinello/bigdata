package bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        Long suma = 0L;
        for (LongWritable val : values) {
            suma += val.get();
        }
        context.write(key, new LongWritable(suma));
    }
}
