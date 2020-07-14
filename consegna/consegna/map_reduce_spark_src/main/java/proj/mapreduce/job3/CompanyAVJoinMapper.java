package proj.mapreduce.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import proj.mapreduce.job2.TagDataWritable;

import java.io.IOException;
import java.util.Arrays;

public class CompanyAVJoinMapper extends Mapper<Object, Text, Text, TagDataWritable> {

    private static final int TICKER = 0;
    private static final int YEAR = 1;
    private static final int VARIATION = 1;
    private static final Text tag = new Text("D1");
    private final Text ticker = new Text();
    private final Text data = new Text();
    private final TagDataWritable tagData = new TagDataWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String[] ticker_year = line[TICKER].split("-");

        try {
            ticker.set(ticker_year[TICKER]);
            data.set(ticker_year[YEAR] + "\t" + line[VARIATION]);
            tagData.set(tag, data);

            context.write(ticker, tagData);
        } catch (Exception e) {
            System.out.println("\nERRORE -> " + Arrays.toString(line));
            System.out.println("ERRORE -> " + Arrays.toString(ticker_year));
            System.out.println();
        }

    }

}
