package lib.input;

import java.io.IOException;


public abstract class RecordReader<Key, Value> {
	
	public RecordReader() { }
	
	public abstract boolean nextKeyValue() throws IOException;
	
	public abstract Key getCurrentKey() throws IOException;
	
	public abstract Value getCurrentValue() throws IOException;
	
	public abstract void close() throws IOException;

}
