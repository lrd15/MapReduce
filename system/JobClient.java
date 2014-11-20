package system;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

import lib.input.InputFormat;
import lib.input.InputSplit;
import lib.input.LineRecordReader;
import lib.input.RecordReader;
import lib.output.FileRecordWriter;
import lib.output.OutputFormat;
import lib.output.RecordWriter;
import mapreduce2.MapContext;
import mapreduce2.Mapper;
import mapreduce2.Partitioner;
import mapreduce2.ReduceContext;
import mapreduce2.Reducer;
import mapreduce2.StringStringIterator;
import config.Job;

public class JobClient {
	
	public JobClient() { }
	
	public void submitJob(Job job) throws IOException, InstantiationException, IllegalAccessException {
		int numSplits = 3; //TODO: hard code here
		InputFormat inputFormat = (InputFormat)job.getInputFormatClass().newInstance(); //for mapper
		OutputFormat outputFormat = (OutputFormat)job.getOutputFormatClass().newInstance(); //for reducer
		
		//map
		InputSplit[] inputSplits = inputFormat.getSplits(job, numSplits); 
		Partitioner partitioner = (Partitioner)job.getPartitionerClass().newInstance();
		int i = 0;
		for(InputSplit split : inputSplits) {
			RecordReader reader = inputFormat.getRecordReader(job, split);
			MapContext mapContext = new MapContext<Long, String, String, String>(job, reader, split, partitioner);
			Mapper mapper = (Mapper)job.getMapperClass().newInstance();
			mapper.run(mapContext); //TODO: create threads
		};

		//reduce
		//int numOfReducer = job.getNumOfReduceJobs();
		ArrayList<LineRecordReader> readers1 = new ArrayList<LineRecordReader>();
		ArrayList<LineRecordReader> readers2 = new ArrayList<LineRecordReader>();
		LineRecordReader reader1 = new LineRecordReader(Paths.get("./wordcount1_0")); //TODO: hard code here
		LineRecordReader reader2 = new LineRecordReader(Paths.get("./wordcount1_1")); //TODO: hard code here
		LineRecordReader reader3 = new LineRecordReader(Paths.get("./wordcount1_2")); //TODO: hard code here
		LineRecordReader reader4 = new LineRecordReader(Paths.get("./wordcount2_0")); //TODO: hard code here
		LineRecordReader reader5 = new LineRecordReader(Paths.get("./wordcount2_1")); //TODO: hard code here
		LineRecordReader reader6 = new LineRecordReader(Paths.get("./wordcount2_2")); //TODO: hard code here
		LineRecordReader reader7 = new LineRecordReader(Paths.get("./wordcount3_0")); //TODO: hard code here
		LineRecordReader reader8 = new LineRecordReader(Paths.get("./wordcount3_1")); //TODO: hard code here
		LineRecordReader reader9 = new LineRecordReader(Paths.get("./wordcount3_2")); //TODO: hard code here
		readers1.add(reader1); readers1.add(reader2); readers1.add(reader3); 
		readers1.add(reader4); readers1.add(reader5); readers1.add(reader6);
		readers2.add(reader7); readers2.add(reader8); readers2.add(reader9);
		
		StringStringIterator itr1 = new StringStringIterator(readers1);
		StringStringIterator itr2 = new StringStringIterator(readers2);
		RecordWriter writer1 = outputFormat.getRecordWriter(job);
		RecordWriter writer2 = outputFormat.getRecordWriter(job);
		ReduceContext reduceContext1 = new ReduceContext<String, String, String, String>(job, itr1, writer1);
		ReduceContext reduceContext2 = new ReduceContext<String, String, String, String>(job, itr2, writer2);
		Reducer reducer1 = (Reducer)job.getReducerClass().newInstance();
		Reducer reducer2 = (Reducer)job.getReducerClass().newInstance();
		reducer1.run(reduceContext1);
		reducer2.run(reduceContext2);
	}
}
