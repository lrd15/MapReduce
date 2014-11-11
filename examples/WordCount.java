package examples;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringTokenizer;

import config.Configuration;
import config.Job;

import lib.input.FixedLengthInputFormat;
import lib.output.FileOutputFormat;
import mapreduce2.*;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, String, String, String> {

		public void map(Object key, String value, MapContext context) throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				context.write(itr.nextToken(), "1");
			}
		}
	}

	public static class IntSumReducer extends Reducer<String, Integer, String, String> {
		// ??
		public void reduce(String key, Iterable<Integer> values, ReduceContext<String, Integer, String, String> context) throws IOException {
			int sum = 0;
			for (Integer val : values) {
				sum += val;
			}
			context.write(key, Integer.toString(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		//TODO: hard code here
		Path path1 = FileSystems.getDefault().getPath("./test/input1");
		Path path2 = FileSystems.getDefault().getPath("./test/");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		
		//job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		//job.setOutputKeyClass(String.class);
		//job.setOutputValueClass(Integer.class);
		job.setRecordSize(11);
		job.setNumOfReduceJobs(1);
		//job.setNumOfMapJobs(1);
		//setPartitionerClass
		
		FixedLengthInputFormat.addInputPaths(job, path1); 
		job.setInputFormatClass(FixedLengthInputFormat.class); //for mapper

		FileOutputFormat.addInputPaths(job, path2);
		job.setOutputFormatClass(FileOutputFormat.class); //for reducer
		
		job.submit();
		
	}
}
