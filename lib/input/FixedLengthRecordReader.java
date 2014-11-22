package lib.input;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FixedLengthRecordReader extends RecordReader<Long, String> {

	private RandomAccessFile file;
	
	private long kstart;
	private long ktotal;
	private long counter;
	
	private Long key;
	private String value;
	private int recordSize;
	
	public FixedLengthRecordReader(String path, InputSplit split, int recordSize) throws IOException {
		FileInputSplit fis = (FileInputSplit)split;
		File fileToRead = new File(path+File.separator+fis.getFilename());
		this.file = new RandomAccessFile(fileToRead, "r");
		long start = fis.getStart();
		file.seek(start);
		long totalLength = fis.getLength();
		this.recordSize = recordSize;

		this.kstart = start / this.recordSize;
		this.ktotal = totalLength / this.recordSize;
		this.counter = 0;
	}
	
	public boolean nextKeyValue() throws IOException {
		//check if all data are read
		if(ktotal > counter) {
			byte[] bytes = new byte[recordSize];
			if (file.read(bytes) != -1) {
				this.key = new Long(kstart+counter);
				this.value = new String(bytes);
				counter++;
				return true;
			}
		}
		this.key = null;
		this.value = null;
		return false;
	}
	
	public Long getCurrentKey() {
		System.out.println("CurrentKey: " + this.key);
		return this.key;
	}
	
	public String getCurrentValue() {
		System.out.println("CurrentValue: " + this.value);
		return this.value;
	}
		
	public void close() throws IOException {
		this.file.close();
	}

}
