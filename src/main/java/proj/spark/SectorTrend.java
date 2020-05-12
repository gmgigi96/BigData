package proj.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.*;

import java.lang.Double;
import java.util.Date;
import java.util.Locale;

import static proj.spark.StockStats.loadCsv;
import static proj.spark.StockStats.toDate;

@Data
@AllArgsConstructor
class StatTrend implements Serializable {
    private double sumVolume;
    private double maxQuotation;
    private double minQuotation;
    private Date maxDate;
    private Date minDate;
    private double sumQuotation;
    private long count;
}


public class SectorTrend {

    private static final int TICKER = 0;
    private static final int CLOSE = 2;
    private static final int VOLUME = 6;
    private static final int DATE = 7;

    private final static int SECTOR = 3;

    private static final int MIN_YEAR = 2008;
    private static final int MAX_YEAR = 2018;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: StockStats <input-file> <input-file> <output-file>");
            System.exit(-1);
        }

        String path_hsp = args[0];
        String path_hs = args[1];
        String path_out = args[2];

        SparkSession spark = SparkSession.builder().appName("SectorTrend").getOrCreate();

        JavaRDD<Row> hsp = loadCsv(spark, path_hsp).javaRDD();
        JavaPairRDD<String, String> hs = loadCsv(spark, path_hs)
                .javaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(TICKER), row.getString(SECTOR)));

        // (TICKER, YEAR) -> (VOLUME, PRICE, PRICE, DATE, DATE, PRICE, 1)
        // (sum_volume, price_min_date, price_max_data, min_date, max_data, close_price, count)
        hsp.mapToPair(row -> {
            Date date = toDate(row.getString(DATE));
            double close_price = Double.parseDouble(row.getString(CLOSE));
            double volume = Double.parseDouble(row.getString(VOLUME));
            return new Tuple2<>(
                    new Tuple2<>(row.getString(TICKER), date.getYear() + 1900),
                    new Tuple7<>(volume, close_price, close_price, date, date, close_price, 1L)
            );
        })
                .filter(t -> t._1._2 >= MIN_YEAR && t._1._2 <= MAX_YEAR)
                // SUM VOLUME, get price at min and max date, sum count, sum price for quotation
                .reduceByKey((t1, t2) -> {
                    double volume = t1._1() + t2._1();
                    double max_quotation, min_quotation;
                    Date max_date, min_date;

                    // quotation at max date of t1 and t2
                    if (t1._5().compareTo(t2._5()) >= 0) {
                        max_quotation = t1._3();
                        max_date = t1._5();
                    } else {
                        max_quotation = t2._3();
                        max_date = t2._5();
                    }

                    // quotation at min date of t1 and t2
                    if (t1._4().compareTo(t2._4()) <= 0) {
                        min_quotation = t1._2();
                        min_date = t1._4();
                    } else {
                        min_quotation = t2._2();
                        min_date = t2._4();
                    }

                    double sum_quotation = t1._6() + t2._6();
                    long count = t1._7() + t2._7();
                    return new Tuple7<>(volume, max_quotation, min_quotation, max_date, min_date, sum_quotation, count);
                })
                // azione -> (anno, volume annuale, variazione annuale=(maxDatePrice-minDatePrice)/minDatePrice*100, quotazione giornaliera=media quotazioni)
                .mapToPair(t -> new Tuple2<>(
                        t._1._1,
                        new Tuple4<>(t._1._2,
                                t._2._1(),
                                (t._2._3() - t._2._2()) / t._2._3() * 100,
                                t._2._6() / t._2._7())
                ))
                // join con hs per settore
                .join(hs)
                // (sector, year) -> (volume, variazione annuale, variazione giornaliera, 1)
                .mapToPair(l -> new Tuple2<>(
                        new Tuple2<>(l._2._2, l._2._1._1()),
                        new Tuple4<>(l._2._1._2(), l._2._1._3(), l._2._1._4(), 1)))
                .reduceByKey((v1, v2) -> new Tuple4<>(
                        v1._1() + v2._1(),      // SOMMA VOLUME
                        v1._2() + v2._2(),      // SOMMA VARIAZIONE ANNUALE
                        v1._3() + v2._3(),      // SOMMA QUOTAZIONE GIORNALIERA
                        v1._4() + v2._4()))     // CONTEGGIO
                // (sector, year) -> (volume medio, variazione annuale media, variazione giornaliera media)
                .mapToPair(t -> new Tuple2<>(
                        t._1,
                        new Tuple3<>(t._2._1() / t._2._4(), t._2._2() / t._2._4(), t._2._3() / t._2._4())
                )).map(t -> String.format(Locale.US,
                "%s,%d,%.2f,%.2f,%.2f",
                t._1._1, t._1._2, t._2._1(), t._2._2(), t._2._3())).saveAsTextFile(path_out);

        spark.close();
    }

}
