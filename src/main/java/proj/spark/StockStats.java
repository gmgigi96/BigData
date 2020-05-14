package proj.spark;

import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple8;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


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

        val filtered_data = hsp.mapToPair(
                row -> {
                    double close_price = Double.parseDouble(row.getString(CLOSE));
                    double volume = Double.parseDouble(row.getString(VOLUME));
                    Date date = toDate(row.getString(DATE));

                    return new Tuple2<>(row.getString(TICKER),
                            new Tuple8<>(
                                    close_price, close_price, volume, 1, date, close_price, date, close_price
                            ));
                })
                .filter(t -> t._2._5().compareTo(MIN_DATE) >= 0 && t._2._5().compareTo(MAX_DATE) <= 0).cache();


        val stats = filtered_data.reduceByKey((t1, t2) -> {

            Date date_min, date_max;
            double close_min_date, close_max_date;

            if (t1._5().compareTo(t2._5()) <= 0) {
                date_min = t1._5();
                close_min_date = t1._6();
            } else {
                date_min = t2._5();
                close_min_date = t2._6();
            }

            if (t1._7().compareTo(t2._7()) >= 0) {
                date_max = t1._7();
                close_max_date = t1._8();
            } else {
                date_max = t2._7();
                close_max_date = t2._8();
            }

            return new Tuple8<>(Double.min(t1._1(), t2._1()), Double.max(t1._2(), t2._2()),
                    t1._3() + t2._3(), t1._4() + t2._4(), date_min, close_min_date, date_max, close_max_date
            );

        });

        JavaRDD<String> ordered_by_variation = stats.mapToPair(v -> {
            double variation = (v._2._8() - v._2._6()) / v._2._6() * 100;
            return new Tuple2<>(variation,
                    String.format(Locale.US, "%s,%.2f,%.2f,%.2f,%.2f",
                            v._1, v._2._1(), v._2._2(), v._2._3() / v._2._4(), variation)
            );
        }).sortByKey(false).map(t -> t._2);

        ordered_by_variation.saveAsTextFile(path_out);

        spark.close();

    }

    public static Dataset<Row> loadCsv(SparkSession spark, String path_hsp) {
        Map<String, String> options = new HashMap<>();
        options.put("header", "true");

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
