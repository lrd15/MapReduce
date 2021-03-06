package lib.input;

import java.io.IOException;

import config.Job;


public abstract class InputFormat<Key, Value> {
	
	public abstract RecordReader<Key, Value> getRecordReader(Job job, String path, InputSplit split) throws IOException;
	
}
