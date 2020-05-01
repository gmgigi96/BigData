package job2;

import job1.StockValues;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AnnualTickerMapper extends Mapper<Object, Text, Text, StockValues> {

    private static final int TICKER = 0;
    private static final int CLOSE = 2;
    private static final int VOLUME = 6;
    private static final int DATE = 7;

    private static final Date MIN_DATE = toDate("2008-01-01");
    private static final Date MAX_DATE = toDate("2018-12-31");

    private final DoubleWritable close_price = new DoubleWritable();
    private final IntWritable volume = new IntWritable();
    private final Text date = new Text();
    private final Text ticker_year = new Text();

    private final StockValues stockValues = new StockValues();

    public static Date toDate(String date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d = null;
        try {
            d = format.parse(date);
        } catch (ParseException e) {

        }
        return d;
    }

    @Override
    protected void map(Object key, Text value, Context context) {
        String[] values = value.toString().split(",");

        try {
            String date = values[DATE];
            String year = date.substring(0, 4);
            Date d = toDate(date);

            if (d.compareTo(MIN_DATE) >= 0 && d.compareTo(MAX_DATE) <= 0) {
                // filter date
                double close_price = Double.parseDouble(values[CLOSE]);
                int volume = Integer.parseInt(values[VOLUME]);

                String ticker = values[TICKER];

                this.ticker_year.set(ticker + "-" + year);
                this.close_price.set(close_price);
                this.volume.set(volume);
                this.date.set(date);

                this.stockValues.set(this.close_price, this.volume, this.date);
                context.write(this.ticker_year, this.stockValues);
            }

        } catch (Exception e) {

        }

    }

}
