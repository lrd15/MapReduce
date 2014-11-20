package lib.output;

import java.io.IOException;


public abstract class RecordWriter<Key, Value> {
	public abstract void write(Key key, Value value) throws IOException;
	public abstract void close() throws IOException;
}
