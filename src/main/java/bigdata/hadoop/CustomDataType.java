package bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class CustomDataType implements Writable {

    private LongWritable bytes;
    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub

    }

}
