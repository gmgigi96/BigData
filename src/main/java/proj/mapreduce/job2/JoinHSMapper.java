package proj.mapreduce.job2;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.stream.StreamSupport;

public class JoinHSMapper extends Mapper<Object, Text, Text, TagDataWritable> {

    private final static int TICKER = 0;
    private final static int SECTOR = 3;

    private final static Text tag = new Text("D2");
    private final TagDataWritable tagData = new TagDataWritable();
    private final Text data = new Text();

    private final Text ticker = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parse = new CSVParser(new StringReader(value.toString()), CSVFormat.INFORMIX_UNLOAD_CSV);
        String[] line = StreamSupport.stream(parse.getRecords().get(0).spliterator(), false).toArray(String[]::new);

        ticker.set(line[TICKER]);
        data.set(line[SECTOR]);
        tagData.set(tag, data);

        context.write(ticker, tagData);
    }
}
