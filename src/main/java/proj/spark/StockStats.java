package proj.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Data
@AllArgsConstructor
class Line implements Serializable {
    private String ticker;
    private double closePrice;
    private long volume;
    private Date date;
}

@Data
@AllArgsConstructor
class Stat implements Serializable {
    private double min;
    private double max;
    private double mean_volume;
    private double variation;
}

public class StockStats {

    private static final int TICKER = 0;
    private static final int CLOSE = 2;
    private static final int VOLUME = 6;
    private static final int DATE = 7;

    private static final Date MIN_DATE = toDate("2008-01-01");
    private static final Date MAX_DATE = toDate("2018-12-31");


    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: StockStats <input-file> <output-file>");
            System.exit(-1);
        }

        String path_hsp = args[0];
        String path_out = args[1];

        SparkSession spark = SparkSession.builder().appName("StockStats").getOrCreate();

        JavaRDD<Row> hsp = loadCsv(spark, path_hsp).javaRDD();

        JavaRDD<Line> filtered_data = hsp.map(row -> new Line(row.getString(TICKER), row.getDouble(CLOSE),
                row.getInt(VOLUME), toDate(row.getString(DATE))))
                .filter(d -> d.getDate().compareTo(MIN_DATE) >= 0 && d.getDate().compareTo(MAX_DATE) <= 0).cache();

        JavaPairRDD<String, Double> ticker_price = filtered_data.mapToPair(d -> new Tuple2<>(d.getTicker(), d.getClosePrice())).cache();

        JavaPairRDD<String, Double> min = getMinClosePrice(ticker_price);
        JavaPairRDD<String, Double> max = getMaxClosePrice(ticker_price);
        JavaPairRDD<String, Double> mean_volume = getMeanVolume(filtered_data);
        JavaPairRDD<String, Double> variation = getVariation(filtered_data);

        JavaPairRDD<String, Stat> stats = join(min, max, mean_volume, variation);

        JavaPairRDD stats_ordered = stats.mapToPair(t -> new Tuple2<>(t._2.getVariation(), new Tuple2(t._1, t._2)))
                .sortByKey(false).mapToPair(t -> t._2);

        saveFile(stats_ordered, path_out);

        spark.close();

    }

    public static void saveFile(JavaPairRDD<String, Stat> rdd, String path) {
        JavaRDD<String> out = rdd.map(t -> String.format(Locale.US,
                "%s,%.2f,%.2f,%.2f,%.2f",
                t._1, t._2.getMin(), t._2.getMax(), t._2.getMean_volume(), t._2.getVariation()));
        out.saveAsTextFile(path);
    }

    private static JavaPairRDD<String, Stat> join(JavaPairRDD<String, Double> min, JavaPairRDD<String, Double> max, JavaPairRDD<String, Double> mean_volume, JavaPairRDD<String, Double> variation) {
        return min.join(max).join(mean_volume).join(variation).mapToPair(t -> {
            val min_max_vol_var = t._2;
            val var = min_max_vol_var._2;
            val min_max_vol = min_max_vol_var._1;
            val vol = min_max_vol._2;
            val min_max = min_max_vol._1;
            val minn = min_max._1;
            val maxx = min_max._2;
            return new Tuple2<>(t._1, new Stat(minn, maxx, vol, var));
        });
    }

    private static JavaPairRDD<String, Double> getVariation(JavaRDD<Line> filtered_data) {
        return filtered_data.mapToPair(d -> new Tuple2<>(d.getTicker(), new Tuple2<>(
                new Tuple2<>(d.getClosePrice(), d.getDate()),
                new Tuple2<>(d.getClosePrice(), d.getDate()))))
                .reduceByKey(StockStats::minmax)
                .mapToPair(ticker_tuple -> {
                    val tuple = ticker_tuple._2;
                    String ticker = ticker_tuple._1;
                    double max_date_price = tuple._2._1;
                    double min_date_price = tuple._1._1;
                    return new Tuple2<>(ticker, (max_date_price - min_date_price) / min_date_price * 100);
                });
    }

    private static JavaPairRDD<String, Double> getMeanVolume(JavaRDD<Line> filtered_data) {
        return filtered_data
                .mapToPair(d -> new Tuple2<>(d.getTicker(), new Tuple2<>(d.getVolume(), 1)))
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
                .mapToPair(ticker_tuple -> new Tuple2<>(ticker_tuple._1, (double) ticker_tuple._2._1 / ticker_tuple._2._2));
    }

    private static JavaPairRDD<String, Double> getMaxClosePrice(JavaPairRDD<String, Double> ticker_price) {
        return ticker_price.reduceByKey(Double::max);
    }

    private static JavaPairRDD<String, Double> getMinClosePrice(JavaPairRDD<String, Double> ticker_price) {
        return ticker_price.reduceByKey(Double::min);
    }

    private static Tuple2<Tuple2<Double, Date>, Tuple2<Double, Date>> minmax(Tuple2<Tuple2<Double, Date>, Tuple2<Double, Date>> t1, Tuple2<Tuple2<Double, Date>, Tuple2<Double, Date>> t2) {
        Tuple2<Double, Date> min, max;
        Tuple2<Double, Date> first1 = t1._1;
        Tuple2<Double, Date> first2 = t2._1;

        Tuple2<Double, Date> second1 = t1._2;
        Tuple2<Double, Date> second2 = t2._2;

        if (first1._2.compareTo(first2._2) <= 0) {
            min = first1;
        } else {
            min = first2;
        }

        if (second1._2.compareTo(second2._2) >= 0) {
            max = second1;
        } else {
            max = second2;
        }

        return new Tuple2<>(min, max);
    }


    public static Dataset<Row> loadCsv(SparkSession spark, String path_hsp) {
        Map<String, String> options = new HashMap<>();
        options.put("header", "true");
        options.put("inferSchema", "true");

        return spark.read().format("csv").options(options).load(path_hsp);
    }

    public static Date toDate(String date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d = null;
        try {
            d = format.parse(date);
        } catch (ParseException ignored) {

        }
        return d;
    }

}
