package lib.input;

import java.io.IOException;
import java.io.Serializable;

import system.Host;

public abstract class InputSplit implements Serializable {
	
	public abstract long getLength() throws IOException;
	
	public abstract Host[] getLocations() throws IOException;
}