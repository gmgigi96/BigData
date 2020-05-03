package job2;

import job1.StockValues;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class AnnualTickerReducer extends Reducer<Text, StockValues, Text, Text> {

    private final Text output = new Text();

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

    @Override
    protected void reduce(Text key, Iterable<StockValues> values, Context context) throws IOException, InterruptedException {

        double annualVolume = 0;
        Date minDate = toDate("2100-12-31");
        double priceAtMinDate = 0;
        Date maxDate = toDate("1900-01-01");
        double priceAtMaxDate = 0;

        double sum = 0;
        int count = 0;

        for (StockValues stockValues : values) {
            int volume = stockValues.getVolume().get();
            double price = stockValues.getClosePrice().get();

            annualVolume += volume;

            Date date = toDate(stockValues.getDate().toString());
            if (date.compareTo(minDate) < 0) {
                minDate = date;
                priceAtMinDate = price;
            }
            if (date.compareTo(maxDate) > 0) {
                maxDate = date;
                priceAtMaxDate = price;
            }

            sum += price;
            count++;
        }

        double variation = (priceAtMaxDate - priceAtMinDate) / priceAtMinDate * 100;

        String output_string = String.format(Locale.US, "%.2f\t%.2f\t%.2f\t%d",
                variation, annualVolume, sum, count
        );

        // ticker-year variation annualVolume sum count

        this.output.set(output_string);
        context.write(key, this.output);
    }
}
