package proj.mapreduce.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SameTrendReducer extends Reducer<Text, Text, Text, Text> {

    private final Text output = new Text();

    @Override
    protected void reduce(Text trend, Iterable<Text> companies, Context context) throws IOException, InterruptedException {

        int count = 0;

        StringBuilder out = new StringBuilder("{");
        for (Text company : companies) {
            out.append(company.toString()).append(",");
            count++;
        }
        out.deleteCharAt(out.length() - 1).append("}");

        output.set(out.toString());

        if (count > 1)
            context.write(output, trend);
    }

}
