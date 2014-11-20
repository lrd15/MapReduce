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
		int i = 0;
		for(InputSplit split : inputSplits) {
			RecordReader reader = inputFormat.getRecordReader(job, split);
			RecordWriter writer = new FileRecordWriter(Paths.get("./"), "/map_output"+(i++));
			MapContext mapContext = new MapContext<>(reader, writer, split);
			Mapper mapper = (Mapper)job.getMapperClass().newInstance();
			mapper.run(mapContext); //TODO: create threads
		};

		//reduce
		ArrayList<LineRecordReader> readers = new ArrayList<LineRecordReader>();
		LineRecordReader reader1 = new LineRecordReader(Paths.get("./map_output0")); //TODO: hard code here
		LineRecordReader reader2 = new LineRecordReader(Paths.get("./map_output1")); //TODO: hard code here
		LineRecordReader reader3 = new LineRecordReader(Paths.get("./map_output2")); //TODO: hard code here
		readers.add(reader1);
		readers.add(reader2);
		readers.add(reader3);
		
		StringStringIterator itr = new StringStringIterator(readers);
		RecordWriter writer = outputFormat.getRecordWriter(job);
		ReduceContext reduceContext = new ReduceContext<>(itr, writer);
		Reducer reducer = (Reducer)job.getReducerClass().newInstance();
		reducer.run(reduceContext);
	}
}
