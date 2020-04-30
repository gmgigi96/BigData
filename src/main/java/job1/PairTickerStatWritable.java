package job1;

import lombok.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class PairTickerStatWritable implements Writable {

    private Text ticker;
    private StockStat stockStat;

    public void set(Text ticker, StockStat stockStat) {
        this.ticker = ticker;
        this.stockStat = stockStat;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ticker.toString());
        stockStat.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ticker = new Text(in.readUTF());
        stockStat = new StockStat();
        stockStat.readFields(in);
    }
}
