package proj.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

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

    private static final Date MIN_DATE = toDate("2008-01-01");
    private static final Date MAX_DATE = toDate("2018-12-31");

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

        JavaRDD<Line> filtered_hsp = hsp.map(row -> new Line(row.getString(TICKER), row.getDouble(CLOSE),
                row.getLong(VOLUME), toDate(row.getString(DATE))))
                .filter(d -> d.getDate().compareTo(MIN_DATE) >= 0 && d.getDate().compareTo(MAX_DATE) <= 0).cache();

        JavaPairRDD<String, Tuple4<Integer, Double, Double, Double>> f = filtered_hsp
                .mapToPair(data -> new Tuple2<>(
                        new Tuple2<>(data.getTicker(), data.getDate().getYear() + 1900), // TODO: change Date with Calendar
                        new StatTrend(data.getVolume(),
                                data.getClosePrice(),
                                data.getClosePrice(),
                                data.getDate(),
                                data.getDate(),
                                data.getClosePrice(),
                                1))
                )
                .reduceByKey((t1, t2) -> {
                    double volume = t1.getSumVolume() + t2.getSumVolume();
                    double max_quotation, min_quotation;
                    Date max_date, min_date;

                    // quotation at max date of t1 and t2
                    if (t1.getMaxDate().compareTo(t2.getMaxDate()) >= 0) {
                        max_quotation = t1.getMaxQuotation();
                        max_date = t1.getMaxDate();
                    } else {
                        max_quotation = t2.getMaxQuotation();
                        max_date = t2.getMaxDate();
                    }

                    // quotation at min date of t1 and t2
                    if (t1.getMinDate().compareTo(t2.getMinDate()) <= 0) {
                        min_quotation = t1.getMinQuotation();
                        min_date = t1.getMinDate();
                    } else {
                        min_quotation = t2.getMinQuotation();
                        min_date = t2.getMinDate();
                    }

                    double sum_quotation = t1.getSumQuotation() + t2.getSumQuotation();
                    long count = t1.getCount() + t2.getCount();
                    return new StatTrend(volume, max_quotation, min_quotation, max_date, min_date, sum_quotation, count);
                })
                .mapToPair(t -> new Tuple2<>(
                        t._1._1,
                        new Tuple4<>(t._1._2,
                                t._2.getSumVolume(),
                                (t._2.getMaxQuotation() - t._2.getMinQuotation()) / t._2.getMinQuotation() * 100,
                                t._2.getSumQuotation() / t._2.getCount())
                        // (anno, volume annuale, variazione annuale, quotazione giornaliera)
                ));

        JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> join = f.join(hs)
                .mapToPair(l -> new Tuple2<>(
                        new Tuple2<>(l._2._2, l._2._1._1()),
                        new Tuple4<>(l._2._1._2(), l._2._1._3(), l._2._1._4(), 1)))
                .reduceByKey((v1, v2) -> new Tuple4<>(
                        v1._1() + v2._1(),      // SOMMA VOLUME
                        v1._2() + v2._2(),      // SOMMA VARIAZIONE ANNUALE
                        v1._3() + v2._3(),      // SOMMA QUOTAZIONE GIORNALIERA
                        v1._4() + v2._4()))     // CONTEGGIO
                .mapToPair(t -> new Tuple2<>(
                        t._1,
                        new Tuple3<>(t._2._1() / t._2._4(), t._2._2() / t._2._4(), t._2._3() / t._2._4())
                ));

        saveFile(join, path_out);

        spark.close();
    }

    private static void saveFile(JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> rdd, String path) {
        JavaRDD<String> out = rdd.map(t -> String.format(Locale.US,
                "%s,%d,%.2f,%.2f,%.2f",
                t._1._1, t._1._2, t._2._1(), t._2._2(), t._2._3()));
        out.saveAsTextFile(path);
    }

}
