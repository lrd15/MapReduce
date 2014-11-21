package lib.input;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import config.Configuration;

import system.Host;


public class FileInputSplit extends InputSplit {
	
	private static final long serialVersionUID = 1L;

	private static int hostItr = 0;
	
	private File file;
	private long start;
	private long length;
	private ArrayList<Host> hosts;
	
	public FileInputSplit(File file, long start, long length){
		this.file = file;
		this.start = start;
		this.length = length;
		this.hosts = Configuration.WORKERS;
	}
	
	public File getFile() throws IOException {
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
