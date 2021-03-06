package lib.output;

import java.io.File;
import java.io.IOException;

import config.Job;


public class FileOutputFormat extends OutputFormat<String, String> {

	@Override
	public RecordWriter<String, String> getRecordWriter(Job job, String outputFileName) throws IOException {
		File path = job.getOutputPath();
		return new FileRecordWriter(path, File.separator + outputFileName);
	}
	
}
