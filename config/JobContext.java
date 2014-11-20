package config;

public interface JobContext {
	public Class getMapperClass();
	public Class getReducerClass();
	//public Class getCombinerClass();
	public Class getPartitionerClass();
	
	public String getJobIdentifier();
	
	public Class getInputFormatClass();
	public Class getOutputFormatClass();
	
	public int getNumOfReduceJobs();
	
}
