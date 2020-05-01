package job2;

import lombok.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
class DataWritable implements Writable {

    private DoubleWritable variation;
    private DoubleWritable volume;
    private DoubleWritable quotation;
    private IntWritable count;

    public void set(DoubleWritable variation, DoubleWritable volume, DoubleWritable quotation, IntWritable count) {
        this.variation = variation;
        this.volume = volume;
        this.quotation = quotation;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(volume.get());
        out.writeDouble(variation.get());
        out.writeDouble(quotation.get());
        out.writeInt(count.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        volume = new DoubleWritable(in.readDouble());
        variation = new DoubleWritable(in.readDouble());
        quotation = new DoubleWritable(in.readDouble());
        count = new IntWritable(in.readInt());
    }
}

public class SectorTrendMapper extends Mapper<Object, Text, Text, DataWritable> {

    private final static int TICKER = 0;
    private final static int YEAR = 1;
    private final static int VARIATION = 2;
    private final static int VOLUME = 3;
    private final static int QUOTATION = 4;
    private final static int COUNT = 5;

    private final Text sectorYear = new Text();
    private final DataWritable data = new DataWritable();

    private final DoubleWritable volume = new DoubleWritable();
    private final DoubleWritable variation = new DoubleWritable();
    private final DoubleWritable quotation = new DoubleWritable();
    private final IntWritable count = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        sectorYear.set(line[TICKER] + "-" + line[YEAR]);

        try {

            double volume = Double.parseDouble(line[VOLUME]);
            double variation = Double.parseDouble(line[VARIATION]);
            double quotation = Double.parseDouble(line[QUOTATION]);
            int count = Integer.parseInt(line[COUNT]);

            this.volume.set(volume);
            this.variation.set(variation);
            this.quotation.set(quotation);
            this.count.set(count);

            data.set(this.variation, this.volume, this.quotation, this.count);
            context.write(sectorYear, data);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
