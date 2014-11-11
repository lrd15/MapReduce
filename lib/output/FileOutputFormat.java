package lib.output;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

import config.Job;


public class FileOutputFormat extends OutputFormat<String, String> {

	static private HashMap<Job, Path> job2Path = new HashMap<Job, Path>();
	
	static public void addInputPaths(Job job, Path path) {
		job2Path.put(job, path);
	}
	
	@Override
	public RecordWriter<String, String> getRecordWriter(Job job, String outputFileName) throws IOException {
		return new FileRecordWriter(job2Path.get(job), outputFileName);
	}
	
}
