package config;

public interface JobContext {
	public Class getMapperClass();
	public Class getReducerClass();
	//public Class getCombinerClass();
	public Class getPartitionerClass();
	
	public Class getInputFormatClass();
	public Class getOutputFormatClass();
	
}
