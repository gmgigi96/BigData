import lombok.*;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class StockValues implements Writable {

    private DoubleWritable closePrice;
    private IntWritable volume;
    private Text date;

    public void set(DoubleWritable closePrice, IntWritable volume, Text date) {
        this.closePrice = closePrice;
        this.volume = volume;
        this.date = date;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(closePrice.get());
        out.writeInt(volume.get());
        out.writeUTF(date.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        closePrice = new DoubleWritable(in.readDouble());
        volume = new IntWritable(in.readInt());
        date = new Text(in.readUTF());
    }
}
