package lib.output;

import java.io.IOException;
import java.nio.file.Path;


public class FileOutputFormat extends OutputFormat<String, String> {

	//static private Job job;
	static private Path outputPath;
	private char delimiter = ',';
	
	public FileOutputFormat() { }
	
	static public void setOutputPath(Path path) {
		outputPath = path;
	}
	
	@Override
	public RecordWriter<String, String> getRecordWriter() throws IOException {
		return new FileRecordWriter(outputPath, delimiter);
	}
	
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}
	
}
