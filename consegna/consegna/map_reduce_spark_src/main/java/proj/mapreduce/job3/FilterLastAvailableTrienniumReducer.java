package proj.mapreduce.job3;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class FilterLastAvailableTrienniumReducer extends Reducer<Text, YearVariation, Text, Text> {

    private final Text output = new Text();

    @Override
    protected void reduce(Text company, Iterable<YearVariation> values, Context context) throws IOException, InterruptedException {

        List<YearVariation> v = new LinkedList<>();

        for (YearVariation yearVariation : values) {
            v.add(new YearVariation(yearVariation));
        }

        if (v.size() >= 3) {
            v.sort((first, second) -> second.getYear().compareTo(first.getYear()));

            List<YearVariation> lastThree = getLastThreeYears(v);

            if (lastThree != null) {

                output.set(lastThree.get(2).toString() + "\t" +
                        lastThree.get(1).toString() + "\t" +
                        lastThree.get(0).toString());
                context.write(company, output);
            }

        }

    }

    private boolean areConsecutive(int a, int b, int c) {
        return a == b + 1 && b == c + 1;
    }

    private List<YearVariation> getLastThreeYears(List<YearVariation> in) {
        if (in.size() >= 3) {
            if (areConsecutive(in.get(0).getYear().get(), in.get(1).getYear().get(), in.get(2).getYear().get())) {
                return Lists.newArrayList(in.get(0), in.get(1), in.get(2));
            } else {
                return getLastThreeYears(in.subList(1, in.size()));
            }
        }
        return null;
    }

}
