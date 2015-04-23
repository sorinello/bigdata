package bigdata.hadoop;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.hive.ql.io.parquet.writable.BigDecimalWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalsReduce extends Reducer<LongWritable, LongWritable, LongWritable, CustomDataType> {

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        Long totalBytes = context.getConfiguration().getLong("TOTAL_BYTES", 0L);
        System.out.println("TOTAL_BYTES FROM TOTALREDUCE: " + totalBytes);
        Long bytes = 0L;

        for (LongWritable val : values) {
            bytes += val.get();
        }

        BigDecimal numerator = new BigDecimal((bytes * 100));
        BigDecimal denominator = new BigDecimal(totalBytes);
        BigDecimal percentage = numerator.divide(denominator, 6, 1);

        CustomDataType cdt = new CustomDataType(new LongWritable(bytes), new BigDecimalWritable(percentage));

        context.write(key, cdt);

    }
}
