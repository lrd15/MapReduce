package config;

import java.io.IOException;
import java.nio.file.Path;

import system.JobClient;


public class Job implements JobContext {
	
	private Configuration config;
	private String identifier;
	private int jobID;
	
	private Class mapperClass;
	private Class combinerClass;
	private Class partitionerClass;
	private Class reducerClass;
	
	private Class outputKeyClass;
	private Class outputValueClass;
	
	private Class inputFormatClass;
	private Class outputFormatClass;

	private Path inputPath;
	private Path outputPath;
	
	//private int numOfReduceJobs;
	//private int numOfMapJobs;
	private int recordSize;
	
	private Job(Configuration config, String identifier) { 
		//Configuration.addDefaultResource("");
		this.config = config;
		this.identifier = identifier;
	}
	
	public static Job getInstance(Configuration config, String identifier) {
		return new Job(config, identifier);
	}
	
	public int getID() {
		return this.jobID;
	}
	
	public void setID(int id) {
		this.jobID = id;
	}
	
	public String getJobIdentifier() {
		return this.identifier;
	}
	
	public void setMapperClass(Class mapperClass) {
		this.mapperClass = mapperClass;
	}
	
	public Class getMapperClass() {
		return this.mapperClass;
	}
	
	public void setCombinerClass(Class combinerClass) {
		this.combinerClass = combinerClass;
	}
	
	public void setReducerClass(Class reducerClass) {
		this.reducerClass = reducerClass;
	}
	
	public Class getReducerClass() {
		return this.reducerClass;
	}
	
	public void setPartitionerClass(Class partitionerClass) {
		this.partitionerClass = partitionerClass;
	}
	
	public Class<?> getPartitionerClass() {
		return this.partitionerClass;
	}

	public void setInputFormatClass(Class<?> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}
	
	public Class<?> getInputFormatClass() {
		return this.inputFormatClass;
	}
	
	public void setOutputFormatClass(Class<?> outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}
	
	public Class getOutputFormatClass() {
		return this.outputFormatClass;
	}
	
	public void setOutputKeyClass(Class outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}
	
	public void setOutputValueClass(Class outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	
	public void setInputPath(Path path) {
		this.inputPath = path;
	}
	
	public void setOutputPath(Path path) {
		this.outputPath = path;
	}
	
	public Path getInputPath() {
		return this.inputPath;
	}
	
	public Path getOutputPath() {
		return this.outputPath;
	}
	
	public void setRecordSize(int recordSize) {
		this.recordSize = recordSize;
	}
	
	public int getRecordSize() {
		return this.recordSize;
	}
	
	public void submit() throws InstantiationException, IllegalAccessException, IOException, ClassNotFoundException {
		new JobClient().submitJob(this);
	}

}