import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockStatisticsMapper extends Mapper<Object, Text, Text, StockValues> {

    private static final int TICKER = 0;
    private static final int CLOSE = 2;
    private static final int VOLUME = 6;
    private static final int DATE = 7;

    private StockValues stockValues = new StockValues();
    private DoubleWritable closePrice = new DoubleWritable(0);
    private IntWritable volume = new IntWritable(0);
    private Text date = new Text();
    private Text ticker = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        ticker.set(line[TICKER]);

        try {
            double close = Double.parseDouble(line[CLOSE]);
            int volume = Integer.parseInt(line[VOLUME]);

            this.date.set(line[DATE]);
            this.closePrice.set(close);
            this.volume.set(volume);

            stockValues.set(this.closePrice, this.volume, this.date);

            context.write(ticker, stockValues);
        } catch (Exception ignored) {

        }
    }
}
