package lib.input;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.StringTokenizer;

public class LineRecordReader extends RecordReader<String, String> {

	private RandomAccessFile file;
	
	private long length;
	private long count;
	private boolean readWholeFile;
	
	private String key;
	private String value;
	
	public LineRecordReader(InputSplit split) throws IOException {
		FileInputSplit fis = (FileInputSplit)split;
		Path path = fis.getFile();
		this.file = new RandomAccessFile(path.toFile(), "r");
		
		long start = fis.getStart();
		file.seek(start);
		this.length = fis.getLength();
		this.count = 0;
		readWholeFile = false;
	}
	
	public LineRecordReader(Path path) throws IOException {
		this.file = new RandomAccessFile(path.toFile(), "r");
		file.seek(0);
		readWholeFile = true;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException {
		//check if all data are read
		if(!readWholeFile && count >= length) {
			this.key = null;
			this.value = null;
			return false;
		}
		
		String line;
		if ((line = file.readLine()) == null) {
			this.key = null;
			this.value = null;
			return false;
		}
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		this.key = tokenizer.nextToken();
		this.value = tokenizer.nextToken();
		
		if(!readWholeFile)
			count += line.length();
		
		return true;
	}

	@Override
	public String getCurrentKey() throws IOException {
		return this.key;
	}

	@Override
	public String getCurrentValue() throws IOException {
		return this.value;
	}

	@Override
	public void close() throws IOException {
		this.file.close();
	}

}
