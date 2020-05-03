package proj.mapreduce.job3;

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
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
class YearVariation implements Writable {

    private IntWritable year;
    private DoubleWritable variation;

    public YearVariation(YearVariation yearVariation) {
        this.year = new IntWritable(yearVariation.year.get());
        this.variation = new DoubleWritable(yearVariation.variation.get());
    }

    public void set(IntWritable year, DoubleWritable variation) {
        this.year = year;
        this.variation = variation;
    }

    @Override
    public String toString() {
        return year.get() + "\t" + variation.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year.get());
        out.writeDouble(variation.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = new IntWritable(in.readInt());
        variation = new DoubleWritable(in.readDouble());
    }
}

public class FilterLastAvailableTrienniumMapper extends Mapper<Object, Text, Text, YearVariation> {

    private static final int COMPANY = 0;
    private static final int YEAR = 1;
    private static final int VARIATION = 2;

    private final Text company = new Text();
    private final YearVariation yearVariation = new YearVariation();
    private final IntWritable year_writable = new IntWritable();
    private final DoubleWritable variation_writable = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");

        company.set(line[COMPANY]);

        //try {

        int year = Integer.parseInt(line[YEAR]);
        double variation = Double.parseDouble(line[VARIATION]);
        year_writable.set(year);
        variation_writable.set(variation);
        yearVariation.set(year_writable, variation_writable);
        System.out.println("[X] " + company.toString() + "  ; " + year + " , " + variation);
        context.write(company, yearVariation);

        //} catch (Exception ignored) { }

    }

}
