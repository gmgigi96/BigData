package proj.mapreduce.job1;

import lombok.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
class DoubleInverseWritable implements WritableComparable<DoubleInverseWritable> {

    private DoubleWritable doubleWritable;

    @Override
    public int compareTo(DoubleInverseWritable other) {
        return other.doubleWritable.compareTo(this.doubleWritable);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        doubleWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        doubleWritable = new DoubleWritable(in.readDouble());
    }
}

public class StatisticsSortMapper extends Mapper<Object, Text, DoubleInverseWritable, PairTickerStatWritable> {

    private static final int VARIATION = 0;
    private static final int MINPRICE = 1;
    private static final int MAXPRICE = 2;
    private static final int MEANVOLUME = 3;

    private PairTickerStatWritable pairTickerStatWritable = new PairTickerStatWritable();
    private StockStat stockStat = new StockStat();
    private Text tickerText = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        String ticker = line[0];
        String[] stats = line[1].split(",");

        try {

            DoubleWritable variation    = new DoubleWritable(Double.parseDouble(stats[VARIATION]));
            DoubleWritable minPrice     = new DoubleWritable(Double.parseDouble(stats[MINPRICE]));
            DoubleWritable maxPrice     = new DoubleWritable(Double.parseDouble(stats[MAXPRICE]));
            DoubleWritable meanVolume   = new DoubleWritable(Double.parseDouble(stats[MEANVOLUME]));

            stockStat.set(variation, minPrice, maxPrice, meanVolume);
            tickerText.set(ticker);
            pairTickerStatWritable.set(tickerText, stockStat);

            DoubleInverseWritable inverseKey = new DoubleInverseWritable(variation);
            context.write(inverseKey, pairTickerStatWritable);

        } catch (Exception ignored) { }

    }
}
