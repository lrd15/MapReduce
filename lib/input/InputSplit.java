package lib.input;

import java.io.IOException;

public abstract class InputSplit {
	
	public InputSplit() {}
	
	public abstract long getTotalLength() throws IOException;
	
	//public abstract String[] getLocations() throws IOException;
}