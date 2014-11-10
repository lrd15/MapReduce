package mapreduce;

import java.io.IOException;

import lib.output.RecordWriter;


public class ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RecordWriter<KEYOUT,VALUEOUT> writer;
	
	public ReduceContext(RecordWriter<KEYOUT,VALUEOUT> writer) {
		this.writer = writer;
	}
	
    public KEYIN getCurrentKey() {
    	return null;
    }

    public VALUEIN getCurrentValue() {
    	return null;
    }

    public Iterable<VALUEIN> getValues() throws IOException {
    	return null;
    }
    
    public boolean nextKeyValue() throws IOException {
    	return false;
    }
    
    public boolean nextKey() throws IOException {
    	return false;
    }

}