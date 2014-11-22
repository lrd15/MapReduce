package lib.output;

import java.io.IOException;

import config.Job;


public abstract class OutputFormat<Key, Value> {

	public abstract RecordWriter<Key, Value> getRecordWriter(Job job) throws IOException;

}
