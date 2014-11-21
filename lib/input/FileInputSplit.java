package lib.input;

import java.io.IOException;
import java.util.ArrayList;

import system.Host;
import config.Configuration;


public class FileInputSplit extends InputSplit {
	
	private static final long serialVersionUID = 1L;

	private static int hostItr = 0;
	
	private String filename;
	private long start;
	private long length;
	private ArrayList<Host> hosts;
	
	public FileInputSplit(String filename, long start, long length){
		this.filename = filename;
		this.start = start;
		this.length = length;
		this.hosts = Configuration.WORKERS;
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
		Host[] host = new Host[]{hosts.get(hostItr)};
		hostItr = (hostItr+1) % hosts.size();
		return host;
	}
	
}
