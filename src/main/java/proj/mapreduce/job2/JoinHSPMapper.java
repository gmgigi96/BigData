package proj.mapreduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinHSPMapper extends Mapper<Object, Text, Text, TagDataWritable> {

    private final static int TICKER = 0;
    private final static int OTHER = 1;
    private final static Text tag = new Text("D1");
    private final Text ticker = new Text();
    private final Text data = new Text();
    private final TagDataWritable tagData = new TagDataWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("-", 2);

        ticker.set(values[TICKER]);
        data.set(values[OTHER]);

        tagData.set(tag, data);

        context.write(ticker, tagData);
    }
}
