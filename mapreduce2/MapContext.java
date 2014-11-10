package mapreduce2;

import java.io.IOException;

import lib.input.InputSplit;
import lib.input.RecordReader;

import lib.output.*;

public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	protected RecordReader<KEYIN, VALUEIN> reader;
	protected RecordWriter<KEYOUT, VALUEOUT> writer;
	protected InputSplit split;

    public MapContext(RecordReader<KEYIN,VALUEIN> reader,
            		  RecordWriter<KEYOUT,VALUEOUT> writer,
            		  InputSplit split) {
    	this.reader = reader;
    	this.writer = writer;
    	this.split = split;
    }
    
    public InputSplit getInputSplit() {
    	return this.split;
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
}