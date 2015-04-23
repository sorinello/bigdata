package bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalsMap extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // String ipAddress = context.getConfiguration().getStrings("IP")[0];
        String line = value.toString();
        String[] splitLine = line.split("\\s+");
        
        // Basically Do NOTHING, just re-transmit the same map
        System.out.println("StatusCode:" + splitLine[0]);
        System.out.println("bytes:" + splitLine[1]);
        LongWritable statusCode = new LongWritable(Long.parseLong(splitLine[0]));
        LongWritable bytes = new LongWritable(Long.parseLong(splitLine[1]));
        context.write(statusCode, bytes);

    }
}
