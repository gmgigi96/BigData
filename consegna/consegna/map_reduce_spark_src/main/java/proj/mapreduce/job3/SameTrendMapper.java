package proj.mapreduce.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;

public class SameTrendMapper extends Mapper<Object, Text, Text, Text> {

    private static final int COMPANY = 0;
    private static final int YEAR1 = 1;
    private static final int VAR1 = 2;
    private static final int YEAR2 = 3;
    private static final int VAR2 = 4;
    private static final int YEAR3 = 5;
    private static final int VAR3 = 6;

    private final Text trend = new Text();
    private final Text company = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");

        company.set(line[COMPANY]);

        int year1 = Integer.parseInt(line[YEAR1]);
        double var1 = Double.parseDouble(line[VAR1]);
        int year2 = Integer.parseInt(line[YEAR2]);
        double var2 = Double.parseDouble(line[VAR2]);
        int year3 = Integer.parseInt(line[YEAR3]);
        double var3 = Double.parseDouble(line[VAR3]);

        String trend_str = String.format(Locale.US, "%d:%.0f,%d:%.0f,%d:%.0f",
                year1, var1, year2, var2, year3, var3);
        trend.set(trend_str);

        context.write(trend, company);
    }
}
