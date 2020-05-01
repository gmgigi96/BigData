package job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

public class SectorTrendReducer extends Reducer<Text, DataWritable, Text, Text> {

    private final Text out = new Text();

    @Override
    protected void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        double sumVolume = 0;
        double sumVariation = 0;
        double sumQuotation = 0;
        int actionCounter = 0;

        for (DataWritable data : values) {
            sumVolume += data.getVolume().get();
            sumVariation += data.getVariation().get();
            sumQuotation += data.getQuotation().get();
            actionCounter += data.getCount().get();
            count++;
        }

        double meanVolume = sumVolume / count;
        double meanVariation = sumVariation / count;
        double meanQuotation = sumQuotation / actionCounter;

        String out_string = String.format(Locale.US, "%.2f\t%.2f\t%.2f",
                meanVolume, meanVariation, meanQuotation
        );

        out.set(out_string);
        context.write(key, out);
    }

}
