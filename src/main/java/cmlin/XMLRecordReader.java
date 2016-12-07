package cmlin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Created by cmlin on 16-11-19.
 */
public class XMLRecordReader extends RecordReader<LongWritable, Text> {
    private long start;
    private long end;
    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private byte[] startTag;
    private byte[] endTag;
    private LongWritable currentKey;
    private Text currentValue;
    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";
    public XMLRecordReader() {}

    public XMLRecordReader(InputSplit split, Configuration context) throws IOException {
        startTag = context.get(START_TAG_KEY).getBytes("UTF-8");
        endTag = context.get(END_TAG_KEY).getBytes("UTF-8");
        FileSplit fileSplit = (FileSplit) split;

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(context);

        fsin = fs.open(fileSplit.getPath());

        fsin.seek(start);
    }

    public void close() throws IOException {
        fsin.close();
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return  currentKey;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    public float getProgress() throws IOException, InterruptedException {
        return (fsin.getPos() - start) / ((float) end - start);
    }

    public void initialize(InputSplit var1, TaskAttemptContext var2) throws IOException, InterruptedException {

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        currentKey = new LongWritable();
        currentValue = new Text();
        return next(currentKey, currentValue);
    }

    private boolean readUntilMatch(byte[] startTag, boolean isWrite) throws IOException {
        int i = 0;
        while(true) {
            int bt = fsin.read();
            if(bt == -1) {
                return false;
            }

            if(isWrite) {
                buffer.write(bt);
            }

            if(bt == startTag[i]) {
                i++;
                if(i >= startTag.length) {
                    return true;
                }
            } else {
                i = 0;
            }

            if(!isWrite && i == 0 && fsin.getPos() >= end) {
                return false;
            }
        }
    }

    private boolean next(LongWritable key, Text value) throws IOException {
        if(fsin.getPos() < end && readUntilMatch(startTag, false)) {
            try {
//                buffer.write(startTag);
                if(readUntilMatch(endTag, true)) {
                    key.set(fsin.getPos());
                    value.set(buffer.getData(), 0, buffer.getLength());
                    return true;
                }
            } finally {
                buffer.reset();
            }
        }
        return false;
    }
}
