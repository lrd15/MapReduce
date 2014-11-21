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
	
	public FixedLengthRecordReader(InputSplit split, int recordSize) throws IOException {
		FileInputSplit fis = (FileInputSplit)split;
		File filename = fis.getFile();
		this.file = new RandomAccessFile(filename, "r");
		
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
		System.out.println("FixedLengthRecordReader: nextKeyValue");
		if(ktotal <= counter) {
			this.key = null;
			this.value = null;
			return false;
		}

		byte[] bytes = new byte[recordSize];
		if (file.read(bytes) != -1) {
			this.key = new Long(kstart+counter);
			this.value = new String(bytes);
			counter++;
		}
		return true;
	}
	
	public Long getCurrentKey() {
		System.out.println("FixedLengthRecordReader: getCurrentKey");
		return this.key;
	}
	
	public String getCurrentValue() {
		System.out.println("FixedLengthRecordReader: getCurrentValue");
		return this.value;
	}
	
	public void close() throws IOException {
		this.file.close();
	}

}
