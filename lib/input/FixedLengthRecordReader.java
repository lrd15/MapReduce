package lib.input;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class FixedLengthRecordReader extends RecordReader<Long, String> {

	private RandomAccessFile file;
	
	private long kstart;
	private long ktotal;
	private long counter;
	
	private Long key;
	private String value;
	private int recordLength;
	
	public FixedLengthRecordReader(InputSplit split) throws IOException {
		FixedLengthInputSplit fis = (FixedLengthInputSplit)split;
		Path path = fis.getPath();
		this.file = new RandomAccessFile(path.toFile(), "r");
		
		long start = fis.getStart();
		file.seek(start);
		long totalLength = fis.getTotalLength();
		this.recordLength = fis.getRecordLength();
		
		this.kstart = start / this.recordLength;
		this.ktotal = totalLength / this.recordLength;
		this.counter = 0;
	}
	
	public boolean nextKeyValue() throws IOException {
		//check if all lines are already read
		if(ktotal == counter) return false;
		
		byte[] bytes = new byte[recordLength];
		if (file.read(bytes) != -1) {
			this.key = new Long(kstart+counter);
			this.value = new String(bytes);
			counter++;
		}
		return true;
	}
	
	public Long getCurrentKey() {
		return this.key;
	}
	
	public String getCurrentValue() {
		return this.value;
	}
	
	public void close() throws IOException {
		this.file.close();
	}

}
