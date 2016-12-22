package cmlin;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordPredict {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, TextIntMapWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word;
		private TextIntMapWritable stripe;
		private IntWritable val;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration mapconf = context.getConfiguration();
			String w1 = mapconf.get("word1");
			String w2 = mapconf.get("word2");
			if(w2.equals("none")) {
				w2 = "";
			}
			byte[] bt = value.getBytes();
			InputStream ip = new ByteArrayInputStream(bt);
			Reader read = new InputStreamReader(ip);
			IKSegmenter iks = new IKSegmenter(read, true);
			Lexeme t;
			String str1="", str2="";
			stripe = new TextIntMapWritable();
			while((t = iks.next()) != null) {
				word = new Text(t.getLexemeText());
//				System.out.print(t.getLexemeText());
				if(str1.equals(w1) && str2.equals(w2) || str1.equals(w1) && w2.length() == 0) {
					stripe.add(word, one);
				}
				if(w2.length() > 0) {
					str1 = str2; str2 = t.getLexemeText();
				} else {
					str1 = t.getLexemeText();
				}

			}
			context.write(new Text("h"), stripe);

		}
	}

	public static class IntSumReducer extends Reducer<Text, TextIntMapWritable, Text, DoubleWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<TextIntMapWritable> stripes, Context context) throws IOException,
				InterruptedException {
			TextIntMapWritable sum = new TextIntMapWritable();
			for (TextIntMapWritable stripe : stripes) {
				sum.merge(stripe);
			}
			double total = 0;
			double maxn = 0;
			Text predictWord = new Text();
			for (TextIntMapWritable.Entry<Writable, Writable> entry: sum.entrySet()) {
				IntWritable val = (IntWritable) entry.getValue();
				if(val.get() > maxn) {
					maxn = val.get();
					Text word = (Text) entry.getKey();
					predictWord.set(word);
				}
				total += val.get();
			}
			context.write(predictWord, new DoubleWritable(maxn / total));
			for (TextIntMapWritable.Entry<Writable, Writable> entry: sum.entrySet()) {
				IntWritable val = (IntWritable) entry.getValue();
				Text word = (Text) entry.getKey();
				context.write(word, new DoubleWritable(val.get() / total));
			}

//			context.write(key, new Text(sum.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		String w1, w2;
		w1 = args[2];
		if(args.length < 4) {
			w2 = "none";
		} else {
			w2 = args[3];
		}
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<content>");
		conf.set("xmlinput.end", "</content");
		conf.set("word1", w1);
		conf.set("word2", w2);
		Job job = Job.getInstance(conf, "WordPredict");
		job.setJarByClass(WordPredict.class);
		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextIntMapWritable.class);
		job.setInputFormatClass(XMLInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
