package job3;


import job2.SectorTrend;
import job2.TagDataWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SameTrendCompany {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path dataset1 = new Path(args[0]);
        Path dataset2 = new Path(args[1]);
        Path out_join = new Path(args[2]);
        Path out_lastThreeYears = new Path(args[3]);
        Path output = new Path(args[4]);

        Job job1 = Job.getInstance(new Configuration());
        job1.setJobName("Join");

        MultipleInputs.addInputPath(job1, dataset1, TextInputFormat.class, CompanyAVJoinMapper.class);
        MultipleInputs.addInputPath(job1, dataset2, TextInputFormat.class, CompanyHSJoinMapper.class);

        FileOutputFormat.setOutputPath(job1, out_join);

        job1.setJarByClass(SectorTrend.class);
        job1.setReducerClass(CompanyJoinReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TagDataWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.waitForCompletion(true);

        // company, year, variation

        // TODO: calcolare media azioni stessa azienda

        Job job2 = Job.getInstance(new Configuration());
        job2.setJobName("Last Three Year trends");

        FileInputFormat.addInputPath(job2, out_join);
        FileOutputFormat.setOutputPath(job2, out_lastThreeYears);

        job2.setJarByClass(SectorTrend.class);
        job2.setMapperClass(FilterLastAvailableTrienniumMapper.class);
        job2.setReducerClass(FilterLastAvailableTrienniumReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(YearVariation.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.waitForCompletion(true);

        // find same trend

        Job job3 = Job.getInstance(new Configuration());
        job3.setJobName("Same Trend");

        FileInputFormat.addInputPath(job3, out_lastThreeYears);
        FileOutputFormat.setOutputPath(job3, output);

        job3.setJarByClass(SectorTrend.class);
        job3.setMapperClass(SameTrendMapper.class);
        job3.setReducerClass(SameTrendReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.waitForCompletion(true);

    }

}
