package lib.input;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.StringTokenizer;

public class LineRecordReader extends RecordReader<String, String> {

	private RandomAccessFile file;
	
	private long length;
	private long count;
	private boolean readWholeFile;
	
	private String key;
	private String value;
	
	public LineRecordReader(String path, InputSplit split) throws IOException {
		FileInputSplit fis = (FileInputSplit)split;
		File fileToRead = new File(path + File.separator + fis.getFilename());
		this.file = new RandomAccessFile(fileToRead, "r");
		
		long start = fis.getStart();
		file.seek(start);
		this.length = fis.getLength();
		this.count = 0;
		readWholeFile = false;
	}
	
	public LineRecordReader(String fileAbsolutePath) throws IOException {
		this.file = new RandomAccessFile(new File(fileAbsolutePath), "r");
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
		
		System.out.println(line);
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
