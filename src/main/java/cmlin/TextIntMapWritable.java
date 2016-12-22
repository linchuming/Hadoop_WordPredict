package cmlin;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

import java.util.Set;

/**
 * Created by cmlin on 16-12-13.
 */
public class TextIntMapWritable extends MapWritable {

    public String toString() {
        String s = new String("{ ");
        Set<Writable> keys = this.keySet();
        for (Writable key : keys) {
            IntWritable count = (IntWritable) this.get(key);
            s = s + key.toString() + "=" + count.toString() + ", ";
        }
        s = s + " }";
        return s;
    }

    public void add(Text key, IntWritable val) {
        if(!this.containsKey(key)) {
            this.put(key, val);
        } else {
            IntWritable old_val = (IntWritable) this.get(key);
            int new_val = old_val.get() + val.get();
            this.put(key, new IntWritable(new_val));
        }
    }

    public void merge(TextIntMapWritable stripe) {
        for(Entry<Writable, Writable> entry: stripe.entrySet()) {
            Text key = (Text)entry.getKey();
            IntWritable val = (IntWritable)entry.getValue();
            this.add(key, val);
        }
    }
}
