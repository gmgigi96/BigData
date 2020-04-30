package job2;

import job1.StockValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SectorTrend {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path dataset1 = new Path(args[0]);
        Path dataset2 = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job = Job.getInstance(new Configuration());
        job.setJobName("Annual");

        FileInputFormat.addInputPath(job, dataset1);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(SectorTrend.class);

        job.setMapperClass(AnnualTickerMapper.class);
        job.setReducerClass(AnnualTickerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StockValues.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }
}
