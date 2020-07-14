package proj.hive.job3;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class Pair {
    public final int v1;
    public final double v2;

    public Pair(int first, double second) {
        this.v1 = first;
        this.v2 = second;
    }
}

@Description(name = "last_three_years", value = "_FUNC_(array<int>, array<double>) - returns last three years trend")
public class LastThreeYearsUDF extends UDF {

    private List<Pair> zip(List<Integer> l1, List<Double> l2) {
        List<Pair> res = new ArrayList<>(l1.size());
        Iterator<Integer> il1 = l1.iterator();
        Iterator<Double> il2 = l2.iterator();

        while (il1.hasNext() && il2.hasNext()) {
            res.add(new Pair(il1.next(), il2.next()));
        }
        return res;
    }

    public String evaluate(List<Integer> years, List<Double> variations) {
        List<Pair> input = zip(years, variations);

        input.sort((p1, p2) -> p2.v1 - p1.v1);
        List<Pair> lastThree = getLastThreeYears(input);

        if (lastThree != null) {
            StringBuilder res = new StringBuilder("{");
            for (Pair t : lastThree) {

                res.append(t.v1).append(':').append((int) t.v2).append(',');

            }
            res.deleteCharAt(res.length() - 1);
            res.append("}");
            return res.toString();
        }
        return "{}";
    }

    private boolean areConsecutive(int a, int b, int c) {
        return a == b + 1 && b == c + 1;
    }

    private List<Pair> getLastThreeYears(List<Pair> in) {
        if (in.size() >= 3) {
            if (areConsecutive(in.get(0).v1, in.get(1).v1, in.get(2).v1)) {
                return Lists.newArrayList(in.get(0), in.get(1), in.get(2));
            } else {
                return getLastThreeYears(in.subList(1, in.size()));
            }
        }
        return null;

    }

}
