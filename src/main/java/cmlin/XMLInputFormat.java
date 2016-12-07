package cmlin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by cmlin on 16-11-19.
 */
public class XMLInputFormat extends TextInputFormat {

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new XMLRecordReader(split, context.getConfiguration());
        } catch (IOException e) {
            return null;
        }
    }
}
