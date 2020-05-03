package proj.mapreduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class JoinReducer extends Reducer<Text, TagDataWritable, Text, Text> {

    private final List<Text> list1 = new LinkedList<>();
    private final List<Text> list2 = new LinkedList<>();

    @Override
    protected void reduce(Text key, Iterable<TagDataWritable> values, Context context) throws IOException, InterruptedException {
        list1.clear();
        list2.clear();

        for (TagDataWritable tagData : values) {
            String tag = tagData.getTag().toString();
            Text data = tagData.getData();

            if (tag.equals("D1")) {
                list1.add(data);
            } else {
                list2.add(data);
            }
        }

        // join between two dataset
        for (Text data : list1) {
            for (Text sector : list2) {
                context.write(sector, data);
            }
        }

    }
}
