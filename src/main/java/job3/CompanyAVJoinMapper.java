package job3;

import job2.TagDataWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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

        ticker.set(ticker_year[TICKER]);
        data.set(ticker_year[YEAR] + "\t" + line[VARIATION]);
        tagData.set(tag, data);

        context.write(ticker, tagData);
    }

}
