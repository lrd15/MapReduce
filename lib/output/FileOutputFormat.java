package lib.output;

import java.io.File;
import java.io.IOException;

import config.Job;


public class FileOutputFormat extends OutputFormat<String, String> {

	@Override
	public RecordWriter<String, String> getRecordWriter(Job job, String outputFileName) throws IOException {
		System.out.println("FileOutputFormat: getRecordWriter");
		File path = job.getOutputPath();
		return new FileRecordWriter(path, outputFileName);
	}
	
}
