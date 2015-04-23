package bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hive.ql.io.parquet.writable.BigDecimalWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class CustomDataType implements Writable {

    private LongWritable bytes;

    private BigDecimalWritable percentage;

    public CustomDataType() {
        bytes = new LongWritable(0);
        percentage = new BigDecimalWritable();
    }

    public CustomDataType(LongWritable bytes, BigDecimalWritable percentage) {
        this.bytes = bytes;
        this.percentage = percentage;

    }

    public LongWritable getBytes() {
        return bytes;
    }

    public void setBytes(LongWritable bytes) {
        this.bytes = bytes;
    }

    public BigDecimalWritable getPercentage() {
        return percentage;
    }

    public void setPercentage(BigDecimalWritable percentage) {
        this.percentage = percentage;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        bytes.readFields(in);
        percentage.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        bytes.write(out);
        percentage.write(out);
    }

    @Override
    public String toString() {
        return "bytes=" + bytes + ", percentage=" + percentage + "%";
    }

}
