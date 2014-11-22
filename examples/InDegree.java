package examples;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import lib.input.FixedLengthInputFormat;
import lib.output.FileOutputFormat;
import mapreduce2.HashPartitioner;
import mapreduce2.MapContext;
import mapreduce2.Mapper;
import mapreduce2.ReduceContext;
import mapreduce2.Reducer;
import config.Job;

public class InDegree {

	public static class DegreeMapper extends Mapper<Long, String, String, String> {
		
		@Override
		public void map(Long key, String value, MapContext<Long, String, String, String> context) throws IOException {
			StringTokenizer itr = new StringTokenizer(value);
			itr.nextToken(); //ignore the source node
			context.write(itr.nextToken(), "1");
		}
	}

	public static class DegreeSumReducer extends Reducer<String, String, String, String> {
		@Override
		public void reduce(String key, Iterable<String> values, ReduceContext<String, String, String, String> context) throws IOException {
			int sum = 0;
			for (String val : values) {
				sum += Integer.valueOf(val);
			}
			context.write(key, Integer.toString(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		File path1 = new File("dg_test/");
		File path2 = new File("dg_output/");
		
		Job job = Job.getInstance("indegree");
		
		job.setMapperClass(DegreeMapper.class);
		job.setReducerClass(DegreeSumReducer.class);
		job.setPartitionerClass(HashPartitioner.class);
		
		job.setRecordSize(16);
		
		job.setInputPath(path1);
		job.setOutputPath(path2);
		
		job.setInputFormatClass(FixedLengthInputFormat.class); //for mapper
		job.setOutputFormatClass(FileOutputFormat.class); //for reducer
		
		job.submit();
		
	}
}
