package bigdata.hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileMap extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String ipAddress = context.getConfiguration().getStrings("IP")[0];
        String line = value.toString();
        String regexIP = String.format("^%s - - (.*) (\\d{3}) (\\d+) \"(.*)$", ipAddress);
        String regexAll = "^(.*) - - (.*) (\\d{3}) (\\d+) \"(.*)$";

        int statusGroup;
        int bytesGroup;
        Pattern pattern;
        if (ipAddress.equals("ALL")) {
            // System.out.println("ALL IPs");
            pattern = Pattern.compile(regexAll);
            statusGroup = 3;
            bytesGroup = 4;

        } else {
            // System.out.println("Specific IP");
            pattern = Pattern.compile(regexIP);
            statusGroup = 2;
            bytesGroup = 3;
        }

        Matcher matcher = pattern.matcher(line);

        if (matcher.find()) {
            System.out.println("Match");

            LongWritable statusCode = new LongWritable(Long.valueOf(matcher.group(statusGroup)));
            LongWritable bytes = new LongWritable(Long.valueOf(matcher.group(bytesGroup)));

            System.out.println("BeforeWritingTOMap: " + statusCode + "--" + bytes);

            context.write(statusCode, bytes);
            context.getCounter("SORINELLO", "TOTAL BYTES").increment(Long.valueOf(matcher.group(bytesGroup)));
        }
    }
}
