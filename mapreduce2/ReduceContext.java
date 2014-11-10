package mapreduce2;

import java.io.IOException;

import lib.output.RecordWriter;

//TODO
public class ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	//protected RecordWriter<KEYOUT,VALUEOUT> writer;
	protected RecordWriter<KEYOUT,VALUEOUT> writer;
	
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
    
    public void write(KEYOUT key, VALUEOUT value) throws IOException {
		this.writer.write(key, value);
	}
}