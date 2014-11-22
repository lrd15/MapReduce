package lib.output;

import java.io.File;
import java.io.IOException;

import config.Job;


public class FileOutputFormat extends OutputFormat<String, String> {

	private static int index = 1;
	
	@Override
	public RecordWriter<String, String> getRecordWriter(Job job) throws IOException {
		System.out.println("FileOutputFormat: getRecordWriter");
		File path = job.getOutputPath();
		return new FileRecordWriter(path, "/hardcode"+(index++));
	}
	
}
