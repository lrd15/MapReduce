package lib.output;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import config.Job;


public abstract class OutputFormat<Key, Value> {
	public abstract RecordWriter<Key, Value> getRecordWriter(Job job, String outputFileName) throws IOException;
	
	public static void main(String [] args) throws IOException {
		Path path = FileSystems.getDefault().getPath("./test/");
		Job job = Job.getInstance(null, "test output");
		FileOutputFormat.addInputPaths(job, path);
		FileOutputFormat outputFormet = new FileOutputFormat();
		RecordWriter recordWriter = outputFormet.getRecordWriter(job, "output1");
		recordWriter.write("test", "value");
		recordWriter.close();
	}
}
