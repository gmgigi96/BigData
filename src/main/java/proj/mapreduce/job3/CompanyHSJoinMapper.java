package proj.mapreduce.job3;

import proj.mapreduce.job2.TagDataWritable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.stream.StreamSupport;

public class CompanyHSJoinMapper extends Mapper<Object, Text, Text, TagDataWritable> {

    private static final int TICKER = 0;
    private static final int COMPANY = 2;
    private static final Text tag = new Text("D2");
    private final Text ticker = new Text();
    private final Text data = new Text();
    private final TagDataWritable tagData = new TagDataWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parse = new CSVParser(new StringReader(value.toString()), CSVFormat.INFORMIX_UNLOAD_CSV);
        String[] line = StreamSupport.stream(parse.getRecords().get(0).spliterator(), false).toArray(String[]::new);

        ticker.set(line[TICKER]);
        data.set(line[COMPANY]);

        tagData.set(tag, data);
        context.write(ticker, tagData);
    }

}
