package job2;

import lombok.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class TagDataWritable implements Writable {

    private Text tag;
    private Text data;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tag.toString());
        out.writeUTF(data.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag = new Text(in.readUTF());
        data = new Text(in.readUTF());
    }

    public void set(Text tag, Text data) {
        this.tag = tag;
        this.data = data;
    }
}
