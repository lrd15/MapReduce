package mapreduce2;

import java.io.IOException;

import lib.input.RecordReader;
import lib.output.*;


public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	protected RecordReader<KEYIN, VALUEIN> reader;
	protected RecordWriter<KEYOUT, VALUEOUT> writer;

    public MapContext(RecordReader<KEYIN,VALUEIN> reader, RecordWriter<KEYOUT,VALUEOUT> writer) {
    	this.reader = reader;
    	this.writer = writer;
    }
    
    public KEYIN getCurrentKey() throws IOException {
    	return this.reader.getCurrentKey();
    }

    public VALUEIN getCurrentValue() throws IOException {
    	return this.reader.getCurrentValue();
    }

    public boolean nextKeyValue() throws IOException {
    	return this.reader.nextKeyValue();
    }
    
    public void write(KEYOUT key, VALUEOUT value) throws IOException {
		this.writer.write(key, value);
	}
}