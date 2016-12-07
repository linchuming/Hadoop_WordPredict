package cmlin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by cmlin on 16-11-19.
 */
public class WordNumber implements WritableComparable<WordNumber> {
    private Text key;
    private IntWritable val;

    public WordNumber() {
        set(new Text(), new IntWritable());
    }

    public WordNumber(Text key, IntWritable val) {
        set(key, val);
    }

    public void set(Text key, IntWritable val) {
        this.key = key;
        this.val = val;
    }

    public Text getWord() {
        return key;
    }

    public IntWritable getNumber() {
        return val;
    }

    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        val.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        val.readFields(dataInput);
    }

    public int compareTo(WordNumber wordNumber) {
        return key.compareTo(wordNumber.key);
    }
}