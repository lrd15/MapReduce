package mapreduce2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lib.input.RecordReader;
import lib.output.FileRecordWriter;
import lib.output.RecordWriter;
import config.Configuration;
import config.JobContext;

public class MapContext<KEYIN, VALUEIN, KEYOUT extends Comparable<KEYOUT>, VALUEOUT> {

	private JobContext jobContext;
	private List<KeyValuePair> output;
	private RecordReader<KEYIN, VALUEIN> reader;
	private Partitioner<KEYOUT, VALUEOUT> partitioner;
	
	private static int instanceID = 0;

    public MapContext(JobContext jobContext,
    				  RecordReader<KEYIN,VALUEIN> reader, 
    				  Partitioner<KEYOUT, VALUEOUT> partitioner) {
    	this.jobContext = jobContext;
    	this.reader = reader;
    	this.partitioner = partitioner;
    	this.output = new ArrayList<KeyValuePair>();
    	instanceID++;
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
    	this.output.add(new KeyValuePair(key, value));
	}
    
    public void close() throws IOException {
    	int numOfReducer = Configuration.NUM_OF_REDUCERS;
    	RecordWriter[] partitionWriters = new RecordWriter[numOfReducer];
    	for(int i=0; i<numOfReducer; i++) {
    		String filename = filenameGenerator(i);
    		partitionWriters[i] = new FileRecordWriter(new File("mapout"), filename);
    	}
    	Collections.sort(this.output);
    	for(KeyValuePair pair : this.output) {
    		KEYOUT key = pair.key;
    		VALUEOUT value = pair.value;
    		int index = this.partitioner.getPartition(key, value, numOfReducer);
    		partitionWriters[index].write(key, value);
    	}
    	for(int i=0; i<numOfReducer; i++) {
    		partitionWriters[i].close();
    	}
		this.reader.close();
	}
    
    private String filenameGenerator(int reducerID) {
    	return "/" + jobContext.getJobIdentifier() + instanceID + "_" + reducerID;
    }
   
    private class KeyValuePair implements Comparable<KeyValuePair>{
    	private KEYOUT key;
    	private VALUEOUT value;
    	
    	public KeyValuePair(KEYOUT k, VALUEOUT v) {
    		this.key = k;
    		this.value = v;
    	}

		@Override
		public int compareTo(KeyValuePair anotherPair) {
			return this.key.compareTo(anotherPair.key);
		}
    }
    
}