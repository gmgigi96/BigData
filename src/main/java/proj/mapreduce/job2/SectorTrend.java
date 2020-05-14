package proj.mapreduce.job2;

import proj.mapreduce.job1.StockValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SectorTrend {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path dataset1 = new Path(args[0]);
        Path dataset2 = new Path(args[1]);
        Path temp1 = new Path(args[2]);
        Path temp2 = new Path(args[3]);
        Path output = new Path(args[4]);

        // ACTIONS AGGREGATION

        Job job = Job.getInstance(new Configuration());
        job.setJobName("Actions aggregation");

        FileInputFormat.addInputPath(job, dataset1);
        FileOutputFormat.setOutputPath(job, temp1);

        job.setJarByClass(SectorTrend.class);

        job.setMapperClass(AnnualTickerMapper.class);
        job.setReducerClass(AnnualTickerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StockValues.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        // JOIN JOB

        Job job1 = Job.getInstance(new Configuration());
        job1.setJobName("Join");
        job1.setJarByClass(SectorTrend.class);

        MultipleInputs.addInputPath(job1, temp1, TextInputFormat.class, JoinHSPMapper.class);
        MultipleInputs.addInputPath(job1, dataset2, TextInputFormat.class, JoinHSMapper.class);

        FileOutputFormat.setOutputPath(job1, temp2);

        job1.setReducerClass(JoinReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TagDataWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.waitForCompletion(true);

        // SECTOR AGGREGATION

        Job job2 = Job.getInstance(new Configuration());
        job2.setJobName("Sector aggregation");

        FileInputFormat.addInputPath(job2, temp2);
        FileOutputFormat.setOutputPath(job2, output);

        job2.setJarByClass(SectorTrend.class);

        job2.setMapperClass(SectorTrendMapper.class);
        job2.setReducerClass(SectorTrendReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DataWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.waitForCompletion(true);

    }
}
