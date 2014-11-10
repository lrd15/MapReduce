package lib.input;

import java.io.IOException;
import java.nio.file.Path;

public class FixedLengthInputSplit extends InputSplit {
	
	private Path filePath;
	private long start;
	private long totalLength;
	private int recordLength;
	//private String[] hosts;
	
	public FixedLengthInputSplit(Path file, long start, long totalLength, int recordLength){
		this.filePath = file;
		this.start = start;
		this.totalLength = totalLength;
		this.recordLength = recordLength;
	}
	
	public Path getPath() throws IOException {
		return this.filePath;
	}
	
	public long getStart() throws IOException {
		return this.start;
	}
	
	public long getTotalLength() throws IOException {
		return this.totalLength;
	}
	
	public int getRecordLength() throws IOException {
		return this.recordLength;
	}	
}
