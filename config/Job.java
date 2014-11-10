package config;

import java.lang.reflect.Constructor;

import lib.input.InputFormat;
import lib.input.InputSplit;
import lib.input.RecordReader;
import lib.output.OutputFormat;
import lib.output.RecordWriter;
import mapreduce2.Mapper;


public class Job {
	
	private Configuration config;
	private String identifier;
	
	private Class mapperClass;
	private Class combinerClass;
	private Class reducerClass;
	private Class outputKeyClass;
	private Class outputValueClass;
	private Class inputFormatClass;
	private Class outputFormatClass;

	private int numOfReduceJobs;
	
	private Job(Configuration config, String identifier) { 
		this.config = config;
		this.identifier = identifier;
	}
	
	public static Job getInstance(Configuration config, String identifier) {
		return new Job(config, identifier);
	}
	
	public void setMapperClass(Class mapperClass) {
		this.mapperClass = mapperClass;
	}
	
	public void setCombinerClass(Class combinerClass) {
		this.combinerClass = combinerClass;
	}
	
	public void setReducerClass(Class reducerClass) {
		this.reducerClass = reducerClass;
	}
	
	public void setInputFormatClass(Class inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}
	
	public void setOutputFormatClass(Class outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}
	
	public void setOutputKeyClass(Class outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}
	
	public void setOutputValueClass(Class outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	
	public void setNumOfReduceJobs(int tasks) {
		this.numOfReduceJobs = tasks;
	}
	
	public void submit() {
		try {
			int numSplits = 10; //TODO: hard code here
			
			Class<?> ifClass = this.inputFormatClass.getClass();
			InputFormat inputFormat = (InputFormat)ifClass.getConstructor().newInstance();
			
			Class<?> ofClass = this.outputFormatClass.getClass();
			OutputFormat outputFormat = (OutputFormat)ofClass.getConstructor().newInstance();
			
			//map
			Class<?> mapperClass = this.mapperClass.getClass();
			Class[] mcontextClass = mapperClass.getDeclaredClasses(); //Mapper.Context
			Constructor mcontextCstr = mcontextClass[0].getConstructor(RecordReader.class, RecordWriter.class);
			InputSplit[] inputSplits = inputFormat.getSplits(numSplits); 
			for(InputSplit split : inputSplits) {
				RecordReader reader = inputFormat.getRecordReader(split);
				RecordWriter writer = outputFormat.getRecordWriter();
				Mapper.Context context = (Mapper.Context)mcontextCstr.newInstance(reader, writer);
				Mapper mapper = (Mapper)mapperClass.getConstructor().newInstance();
				mapper.run(context); //TODO: threading
			};

			//reduce
			Class<?> reducerClass = this.reducerClass.getClass();
			for(int i=0; i<this.numOfReduceJobs; i++) {
				//Reducer reducer = (Reducer)reducerClass.getConstructor().newInstance();
				//reducer.run();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
}
