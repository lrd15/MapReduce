package lib.input;

import java.io.IOException;

public abstract class InputSplit {
	
	public InputSplit() {}
	
	public abstract long getLength() throws IOException;
	
	//public abstract String[] getLocations() throws IOException;
}