package lib.input;

import java.io.IOException;


public class FileInputSplit extends InputSplit {
	
	private long start;
	private long length;
	//private String[] hosts;
	
	public FileInputSplit(long start, long length){
		this.start = start;
		this.length = length;
	}
	
	public long getStart() throws IOException {
		return this.start;
	}
	
	public long getLength() throws IOException {
		return this.length;
	}
	
}
