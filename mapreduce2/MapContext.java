package mapreduce2;

import java.io.IOException;

import config.Configuration;

import lib.input.RecordReader;
import lib.output.*;


public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private Configuration config;
	private RecordReader<KEYIN, VALUEIN> reader;
	private RecordWriter<KEYOUT, VALUEOUT> writer;

    public MapContext(Configuration config, RecordReader<KEYIN,VALUEIN> reader, RecordWriter<KEYOUT,VALUEOUT> writer) {
    	this.config = config;
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
		//TODO sort
    	this.writer.write(key, value);
	}
    
    public void close() throws IOException {
		this.writer.close();
		this.reader.close();
	}
   
}