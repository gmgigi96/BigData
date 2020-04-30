package job1;

import lombok.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class StockStat implements WritableComparable<StockStat> {

    private DoubleWritable variation;
    private DoubleWritable minPrice;
    private DoubleWritable maxPrice;
    private DoubleWritable meanVolume;

    public void set(DoubleWritable variation, DoubleWritable minPrice, DoubleWritable maxPrice, DoubleWritable meanVolume) {
        this.variation = variation;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
        this.meanVolume = meanVolume;
    }

    @Override
    public int compareTo(StockStat o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        variation.write(out);
        minPrice.write(out);
        maxPrice.write(out);
        meanVolume.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        variation = new DoubleWritable(in.readDouble());
        minPrice = new DoubleWritable(in.readDouble());
        maxPrice = new DoubleWritable(in.readDouble());
        meanVolume = new DoubleWritable(in.readDouble());
    }
}
