package examples;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.StringTokenizer;

import config.Configuration;
import config.Job;

import lib.input.FixedLengthInputFormat;
import lib.output.FileOutputFormat;
import mapreduce2.*;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, String, String, String> {

		public void map(Object key, String value, Context context) throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				context.write(itr.nextToken(), "1");
			}
		}
	}

	public static class IntSumReducer extends Reducer<String, Iterable<Integer>, String, String> {

		public void reduce(String key, Iterable<Integer> values, Context context) throws IOException {
			int sum = 0;
			for (Integer val : values) {
				sum += val;
			}
			context.write(key, Integer.toString(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		int recordSize = 10;
		FixedLengthInputFormat.setRecordSize(recordSize);
		FixedLengthInputFormat.setInputPath(Paths.get(args[0]));
		FileOutputFormat.setOutputPath(Paths.get(args[1]));
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		
		//job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(Integer.class);
		job.setInputFormatClass(FixedLengthInputFormat.class);
		job.setOutputFormatClass(FileOutputFormat.class);
		job.submit();
		
	}
}
