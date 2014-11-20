package lib.input;

import java.io.IOException;
import java.nio.file.Path;

import system.Host;


public class FileInputSplit extends InputSplit {
	
	private Path file;
	private long start;
	private long length;
	//private String[] hosts;
	
	public FileInputSplit(Path file, long start, long length){
		this.file = file;
		this.start = start;
		this.length = length;
	}
	
	public Path getFile() throws IOException {
		return this.file;
	}
	
	public long getStart() throws IOException {
		return this.start;
	}
	
	public long getLength() throws IOException {
		return this.length;
	}

	@Override
	public Host[] getLocations() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
