package lib.output;

import java.io.IOException;
import java.nio.file.Path;

public class FileOutputFormat extends OutputFormat<String, String> {

	private Path outputPath;
	private char delimiter;
	
	public FileOutputFormat(Path path) {
		this.outputPath = path;
		this.delimiter = ',';
	}
	
	@Override
	public RecordWriter<String, String> getRecordWriter() throws IOException {
		return new FileRecordWriter(outputPath, delimiter);
	}

//	public void setOutputPath(Path path) {
//		this.outputPath = path;
//	}
	
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}
	
	public char getDelimiter() {
		return this.delimiter;
	}
}
