package lib.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

public class FileRecordWriter extends RecordWriter<String, String> {

	private PrintWriter writer = null;
	private char delimiter = ' ';

	public FileRecordWriter(File dir, String outputFileName) {
		if (!dir.exists())
			dir.mkdirs();
		try {
			this.writer = new PrintWriter(dir.toString() + File.separator + outputFileName);
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
