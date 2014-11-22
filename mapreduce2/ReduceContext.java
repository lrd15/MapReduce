package mapreduce2;

import java.io.IOException;

import config.JobContext;

import lib.output.RecordWriter;

public class ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RawKeyValueIterator<KEYIN, VALUEIN> keyValueIterator;
	private RecordWriter<KEYOUT,VALUEOUT> writer;
	
	public ReduceContext(JobContext jobContext, RawKeyValueIterator<KEYIN,VALUEIN> keyValueIterator, RecordWriter<KEYOUT,VALUEOUT> writer) {
		this.keyValueIterator = keyValueIterator;
		this.writer = writer;
	}
	
    public KEYIN getCurrentKey() throws IOException {
    	return this.keyValueIterator.getKey();
    }

    public Iterable<VALUEIN> getValues() throws IOException {
    	return this.keyValueIterator.getValue();
    }
    
    public boolean nextKey() throws IOException {
    	return this.keyValueIterator.next();
    }
    
    public void write(KEYOUT key, VALUEOUT value) throws IOException {
    	this.writer.write(key, value);
    }
    
    public void close() throws IOException {
    	this.keyValueIterator.close();
    	this.writer.close();
    }
    
}