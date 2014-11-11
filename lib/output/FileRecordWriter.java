package lib.output;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

public class FileRecordWriter extends RecordWriter<String, String> {

	private PrintWriter writer = null;
	private char delimiter = ',';
	
	public FileRecordWriter(Path filePath, String outputFileName) {
		try {
			this.writer =  new PrintWriter(filePath.toString()+outputFileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void write(String key, String value) throws IOException {
		this.writer.println(key + delimiter + value);
	}

	@Override
	public void close() throws IOException {
		this.writer.close();
	}

}
