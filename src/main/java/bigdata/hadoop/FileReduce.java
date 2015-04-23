package bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        Long suma = 0L;

        for (LongWritable val : values) {
            suma += val.get();
        }

        // Long totalBytes = context.getCounter("SORINELLO", "TOTAL BYTES").getValue();
        // System.out.println("COUNTER: " + totalBytes);

        context.write(key, new LongWritable(suma));
    }
}
