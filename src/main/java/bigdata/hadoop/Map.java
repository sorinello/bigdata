package bigdata.hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // System.out.println("LINIE/KEY: " + key);
        // System.out.println("LINIA/VALUE: " + value);
        String ipAddress = context.getConfiguration().getStrings("IP")[0];
        // System.out.println("IP: " + ipAddress);

        String line = value.toString();
        String regex = String.format("^%s - - (.*) (\\d{3}) (\\d+) \"(.*)$", ipAddress);
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);

        if (matcher.find()) {

            IntWritable statusCode = new IntWritable(Integer.parseInt(matcher.group(2)));
            LongWritable bytes = new LongWritable(Long.valueOf(matcher.group(3)));

            new IntWritable(Integer.parseInt(matcher.group(2)));
            System.out.println("BeforeWritingTOMap: " + statusCode + "--" + bytes);
            context.write(statusCode, bytes);
        }
    }

}
