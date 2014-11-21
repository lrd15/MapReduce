package lib.input;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import config.Configuration;

import system.Host;


public class FileInputSplit extends InputSplit {
	
	private static int hostItr = 0;
	
	private Path file;
	private long start;
	private long length;
	private ArrayList<Host> hosts;
	
	public FileInputSplit(Path file, long start, long length){
		this.file = file;
		this.start = start;
		this.length = length;
		this.hosts = Configuration.WORKERS;
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
		Host[] host = new Host[]{hosts.get(hostItr)};
		hostItr = (hostItr+1) % hosts.size();
		return host;
	}
	
}
