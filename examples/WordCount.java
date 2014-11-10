package examples;

import java.io.IOException;
import java.util.StringTokenizer;

import lib.input.FixedLengthInputFormat;
import lib.input.FixedLengthInputSplit;
import mapreduce2.*;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, String, String, Integer> {

		private final static Integer one = new Integer(1);

		public void map(Object key, String value, Context context)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				context.write(itr.nextToken(), one);
			}
		}

	}

	public static class IntSumReducer extends Reducer<String, Iterable<Integer>, String, Integer> {

		public void reduce(String key, Iterable<Integer> values, Context context) 
				throws IOException {
			int sum = 0;
			for (Integer val : values) {
				sum += val;
			}
			context.write(key, sum);
		}

	}

	public static void main(String[] args) throws Exception {
		//Configuration conf = new Configuration();
		//Job job = Job.getInstance(conf, "word count");
		Job job = Job.getInstance();
		//job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(Integer.class);
		//FixedLengthInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
