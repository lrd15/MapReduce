package mapreduce2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import lib.input.InputSplit;
import lib.input.RecordReader;
import lib.output.*;


public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private TreeMap<KEYOUT, VALUEOUT> output;
	private RecordReader<KEYIN, VALUEIN> reader;
	private RecordWriter<KEYOUT, VALUEOUT> writer;
	private InputSplit inputSplit;

    public MapContext(RecordReader<KEYIN,VALUEIN> reader, RecordWriter<KEYOUT,VALUEOUT> writer, InputSplit inputSplit) {
    	this.reader = reader;
    	this.writer = writer;
    	this.inputSplit = inputSplit;
    	this.output = new TreeMap<KEYOUT, VALUEOUT>();
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
    
    public InputSplit getInputSplit() {
    	return this.inputSplit;
    }
    
    public void write(KEYOUT key, VALUEOUT value) throws IOException {
    	this.output.put(key, value);
	}
    
    public void close() throws IOException {
    	for(Map.Entry<KEYOUT, VALUEOUT> entry : output.entrySet()) {
    		this.writer.write(entry.getKey(), entry.getValue());
    	}
    	this.writer.close();
		this.reader.close();
	}
   
}