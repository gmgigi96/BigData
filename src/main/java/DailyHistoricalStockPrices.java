import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class DailyHistoricalStockPrices {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job1 = Job.getInstance(new Configuration());
        job1.setJobName("DailyHistoricalStockPrices");

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.setJarByClass(DailyHistoricalStockPrices.class);

        job1.setMapperClass(StockStatisticsMapper.class);
        job1.setReducerClass(StockStatisticsReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(StockValues.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.waitForCompletion(true);



        Job job2 = Job.getInstance(new Configuration());
        job2.setJobName("DailyHistoricalStockPrices");

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setJarByClass(DailyHistoricalStockPrices.class);

        job2.setMapperClass(StatisticsSortMapper.class);
        job2.setReducerClass(StatisticsSortReducer.class);

        job2.setMapOutputKeyClass(DoubleInverseWritable.class);
        job2.setMapOutputValueClass(PairTickerStatWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.waitForCompletion(true);



    }
}
