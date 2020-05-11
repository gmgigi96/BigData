package proj.spark;

import com.google.common.collect.Lists;
import lombok.val;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple6;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static proj.spark.StockStats.loadCsv;
import static proj.spark.StockStats.toDate;

public class SameTrend {

    private static final int TICKER = 0;
    private static final int CLOSE = 2;
    private static final int DATE = 7;
    private static final int COMPANY = 2;

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: SameTrend <input-file> <input-file> <output-file>");
            System.exit(-1);
        }

        String path_hsp = args[0];
        String path_hs = args[1];
        String path_output = args[2];

        SparkSession spark = SparkSession.builder().appName("SameTrend").getOrCreate();

        JavaPairRDD<String, String> hs = loadCsv(spark, path_hs)
                .javaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(TICKER), row.getString(COMPANY)));

        JavaRDD<Row> hsp = loadCsv(spark, path_hsp).javaRDD();

        JavaPairRDD<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Iterable<String>> sameTrend = hsp
                // (TICKER, YEAR) -> (CLOSE_MAX_DATE = CLOSE, MAX_DATE=DATE, CLOSE_MIN_DATE=CLOSE, MIN_DATE=DATE)
                .mapToPair(row -> new Tuple2<>(
                        new Tuple2<>(row.getString(TICKER), toDate(row.getTimestamp(DATE).toString()).getYear() + 1900),
                        new Tuple4<>(
                                row.getDouble(CLOSE),
                                toDate(row.getTimestamp(DATE).toString()),
                                row.getDouble(CLOSE),
                                toDate(row.getTimestamp(DATE).toString())
                        )))
                // calculate close at max date and close at min date
                .reduceByKey((t1, t2) -> {
                    double close_max_date, close_min_date;
                    Date max_date, min_date;

                    if (t1._2().compareTo(t2._2()) >= 0) {
                        max_date = t1._2();
                        close_max_date = t1._1();
                    } else {
                        max_date = t2._2();
                        close_max_date = t2._1();
                    }

                    if (t1._4().compareTo(t2._4()) <= 0) {
                        min_date = t1._4();
                        close_min_date = t1._3();
                    } else {
                        min_date = t2._4();
                        close_min_date = t2._3();
                    }

                    return new Tuple4<>(close_max_date, max_date, close_min_date, min_date);
                })
                // TICKER -> (YEAR, ANNUAL VARIATION)
                .mapToPair(t -> {
                    val ticker_year = t._1;
                    val prices = t._2;
                    double close_max_price = prices._1();
                    double close_min_price = prices._3();
                    double ann_var = (close_max_price - close_min_price) / close_min_price * 100;

                    return new Tuple2<>(ticker_year._1, new Tuple2<>(ticker_year._2, ann_var));
                })
                // TICKER -> ((YEAR, ANNUAL VARIATION), COMPANY)
                .join(hs)
                // TODO: media delle azioni della stessa azienda
                // COMPANY -> (YEAR, ANNUAL VARIATION)
                .mapToPair(t -> {
                    val data = t._2;
                    Tuple2<Integer, Integer> key = new Tuple2<>(data._1._1, (int) data._1._2.doubleValue());

                    return new Tuple2<>(data._2, key);
                })
                .groupByKey()
                // get last triennium => COMPANY -> [(YEAR-2, VAR), (YEAR-1, VAR), (YEAR, VAR)]
                .mapToPair(t -> {
                    String company = t._1;
                    val list = StreamSupport
                            .stream(t._2.spliterator(), false).sorted((t1, t2) -> t2._1 - t1._1).collect(Collectors.toList());
                    List<Tuple2<Integer, Integer>> lastThree = getLastThreeYears(list);
                    return new Tuple2<>(company, lastThree);
                })
                // filter triennio available
                .filter(t -> t._2 != null)
                // (YEAR-2, VAR, YEAR-1, VAR, YEAR, VAR) -> COMPANY
                .mapToPair(t -> {
                    val list = t._2;
                    val key = new Tuple6<>(list.get(0)._1, list.get(0)._2,
                            list.get(1)._1, list.get(1)._2,
                            list.get(2)._1, list.get(2)._2);
                    return new Tuple2<>(key, t._1);
                })
                // group by same trend
                .groupByKey()
                .filter(t -> StreamSupport
                        .stream(t._2.spliterator(), false).count() > 1);

        saveFile(sameTrend, path_output);

        spark.close();
    }

    private static void saveFile(JavaPairRDD<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Iterable<String>> sameTrend, String path) {
        JavaRDD<String> save = sameTrend.map(t -> {
            String company_string = StreamSupport.stream(t._2.spliterator(), false).collect(Collectors.joining(","));
            val trend = t._1;

            return String.format("{%s}:%d:%d,%d:%d,%d:%d", company_string, trend._1(), trend._2(),
                    trend._3(), trend._4(), trend._5(), trend._6());
        }).coalesce(1);
        save.saveAsTextFile(path);
    }

    private static List<Tuple2<Integer, Integer>> getLastThreeYears(List<Tuple2<Integer, Integer>> in) {
        if (in.size() >= 3) {
            if (areConsecutive(in.get(0)._1, in.get(1)._1, in.get(2)._1)) {
                return Lists.newArrayList(in.get(0), in.get(1), in.get(2));
            } else {
                return getLastThreeYears(in.subList(1, in.size()));
            }
        }
        return null;
    }

    private static boolean areConsecutive(int a, int b, int c) {
        return a == b + 1 && b == c + 1;
    }

}
