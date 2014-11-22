package lib.input;

import java.io.IOException;

import system.Host;


public class FileInputSplit extends InputSplit {
	
	private static final long serialVersionUID = 1L;

	private String filename;
	private long start;
	private long length;
	private Host[] hosts;
	
	public FileInputSplit(String filename, long start, long length, Host[] hosts){
		this.filename = filename;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}
	
	public String getFilename() throws IOException {
		return this.filename;
	}
	
	public long getStart() throws IOException {
		return this.start;
	}
	
	public long getLength() throws IOException {
		return this.length;
	}

	@Override
	public Host[] getLocations() throws IOException {
		return this.hosts;
	}
	
}
