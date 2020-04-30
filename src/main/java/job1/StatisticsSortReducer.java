package job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StatisticsSortReducer extends Reducer<DoubleInverseWritable, PairTickerStatWritable, Text, Text> {

    private Text stats = new Text();

    @Override
    protected void reduce(DoubleInverseWritable key, Iterable<PairTickerStatWritable> values, Context context) throws IOException, InterruptedException {
        for (PairTickerStatWritable pair : values) {
            StockStat stockStat = pair.getStockStat();

            double variation = stockStat.getVariation().get();
            double minPrice = stockStat.getMinPrice().get();
            double maxPrice = stockStat.getMaxPrice().get();
            double meanVolume = stockStat.getMeanVolume().get();

            stats.set(variation + "\t" + minPrice + "\t" + maxPrice + "\t" + meanVolume);
            context.write(pair.getTicker(), stats);
        }
    }

}
