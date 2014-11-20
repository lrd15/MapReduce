package lib.output;

import java.io.IOException;
import java.nio.file.Path;

import config.Job;


public class FileOutputFormat extends OutputFormat<String, String> {

	//TODO: remove hardcode
	private static int index = 1;
	
	@Override
	public RecordWriter<String, String> getRecordWriter(Job job) throws IOException {
		Path path = job.getOutputPath();
		return new FileRecordWriter(path, "/hardcode"+(index++));
	}
	
}
