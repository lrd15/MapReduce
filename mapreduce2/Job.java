package mapreduce2;

import java.io.IOException;

public class Job {
	
	private Class mapperClass;
	private Class combinerClass;
	private Class reducerClass;
	private Class outputKeyClass;
	private Class outputValueClass;
	
	public Job() { }
	
	public static Job getInstance() {
		return new Job();
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
	
	public void setOutputKeyClass(Class outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}
	
	public void setOutputValueClass(Class outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	
}
