package lib.input;

import java.io.IOException;

import system.Host;

public abstract class InputSplit {
	
	public abstract long getLength() throws IOException;
	
	public abstract Host[] getLocations() throws IOException;
}