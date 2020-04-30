package job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StockStatisticsReducer extends Reducer<Text, StockValues, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<StockValues> values, Context context) throws IOException, InterruptedException {
        double minPrice = Double.MAX_VALUE;
        double maxPrice = Double.MIN_VALUE;
        int count = 0;
        double sumVolumes = 0;
        Date minDate = toDate("2100-12-31");
        double priceAtMinDate = 0;
        Date maxDate = toDate("1900-01-01");
        double priceAtMaxDate = 0;

        for (StockValues stockValues : values) {

            double price = stockValues.getClosePrice().get();
            minPrice = Math.min(minPrice, price);
            maxPrice = Math.max(maxPrice, price);

            count++;
            sumVolumes += stockValues.getVolume().get();

            Date date = toDate(stockValues.getDate().toString());
            if (date.compareTo(minDate) < 0) {
                minDate = date;
                priceAtMinDate = price;
            }
            if (date.compareTo(maxDate) > 0) {
                maxDate = date;
                priceAtMaxDate = price;
            }

        }

        double meanVolume = sumVolumes / count;
        double variation = (priceAtMaxDate - priceAtMinDate) / priceAtMinDate * 100;

        // variation, minPrice, maxPrice, meanVolume
        result.set(variation + "," + minPrice + "," + maxPrice + "," + meanVolume);

        context.write(key, result);

    }

    public static Date toDate(String date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d = null;
        try {
            d = format.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return d;
    }

}
