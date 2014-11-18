package lib.output;

import java.io.IOException;
import java.nio.file.Path;

import config.Job;


public class FileOutputFormat extends OutputFormat<String, String> {

	@Override
	public RecordWriter<String, String> getRecordWriter(Job job, String outputFileName) throws IOException {
		Path path = job.getOutputPath();
		return new FileRecordWriter(path, outputFileName);
	}
	
}
