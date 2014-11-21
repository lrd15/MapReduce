package lib.input;

import java.io.IOException;
import java.io.Serializable;

import system.Host;

public abstract class InputSplit implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public abstract long getLength() throws IOException;
	
	public abstract Host[] getLocations() throws IOException;
}