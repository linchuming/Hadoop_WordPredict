package cmlin;

import java.io.IOException;

import java.util.LinkedList;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Compute {

    public static class TokenizerMapper extends Mapper<Text, Text, Text, WordNumber> {

        private Text word = new Text();
        private WordNumber val = new WordNumber();
        private IntWritable number = new IntWritable();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            word.set("");
            number.set(Integer.parseInt(value.toString()));
            val.set(key, number);
            context.write(word, val);
        }
    }

    public static class IntSumReducer extends Reducer<Text, WordNumber, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<WordNumber> values, Context context) throws IOException,
                InterruptedException {
            long sum = 0;
            int maxn = 0;
            Text predict = new Text();
            LinkedList<WordNumber> tmp = new LinkedList<WordNumber>();
            for(WordNumber val : values) {
                int num = val.getNumber().get();
                tmp.add(new WordNumber(new Text(val.getWord()), new IntWritable(num)));
                sum += num;
                if(num > maxn) {
                    maxn = num;
                    predict = new Text(val.getWord());
                }
            }
            result.set(0);
            context.write(predict, result);
            for(WordNumber val : tmp) {
                double p = val.getNumber().get() / (double) sum;
                result.set(p);
                context.write(val.getWord(), result);
            }


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Compute");
        job.setJarByClass(WordPredict.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WordNumber.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
