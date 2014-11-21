package lib.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

public class FileRecordWriter extends RecordWriter<String, String> {

	private PrintWriter writer = null;
	private char delimiter = ' ';

	public FileRecordWriter(Path path, String outputFileName) {
		File dir = new File(path.toString());
		if (!dir.exists())
			dir.mkdir();
		try {
			this.writer = new PrintWriter(dir.toString() + outputFileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(String key, String value) throws IOException {
		System.out.println("FileRecordWriter: write");
		this.writer.println(key + delimiter + value);
	}

	@Override
	public void close() throws IOException {
		this.writer.close();
	}

}
