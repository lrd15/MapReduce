package lib.output;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

public class FileRecordWriter extends RecordWriter<String, String> {

	private PrintWriter writer = null;
	private char delimiter;
	
	public FileRecordWriter(Path filePath, char delimiter) {
		try {
			this.writer =  new PrintWriter(filePath.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.delimiter = delimiter;
	}
	
	@Override
	public void write(String key, String value) throws IOException {
		this.writer.print(key);
		this.writer.print(delimiter);
		this.writer.print(value);
	}

	@Override
	public void close() throws IOException {
		this.writer.close();
	}

}
