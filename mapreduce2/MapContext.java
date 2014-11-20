package mapreduce2;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

import config.Configuration;
import config.JobContext;

import lib.input.InputSplit;
import lib.input.RecordReader;
import lib.output.*;

public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private JobContext jobContext;
	private TreeMap<KEYOUT, VALUEOUT> output;
	private RecordReader<KEYIN, VALUEIN> reader;
	private Partitioner<KEYOUT, VALUEOUT> partitioner;
	private InputSplit inputSplit;
	
	private static int id = 0;

    public MapContext(JobContext jobContext,
    				  RecordReader<KEYIN,VALUEIN> reader, 
    				  //RecordWriter<KEYOUT,VALUEOUT> writer, 
    				  InputSplit inputSplit, 
    				  Partitioner<KEYOUT, VALUEOUT> partitioner) {
    	this.jobContext = jobContext;
    	this.reader = reader;
    	this.inputSplit = inputSplit;
    	this.partitioner = partitioner;
    	this.output = new TreeMap<KEYOUT, VALUEOUT>();
    	id++;
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
    	int numOfReducer = Configuration.NUM_OF_REDUCERS;
    	RecordWriter[] partitionWriters = new RecordWriter[numOfReducer];
    	for(int i=0; i<numOfReducer; i++) {
    		String filename = "/"+jobContext.getJobIdentifier()+id+"_"+i;
    		partitionWriters[i] = new FileRecordWriter(Paths.get("./"), filename);
    	}
    	for(Map.Entry<KEYOUT, VALUEOUT> entry : output.entrySet()) {
    		KEYOUT key = entry.getKey();
    		VALUEOUT value = entry.getValue();
    		int index = this.partitioner.getPartition(key, value, numOfReducer);
    		partitionWriters[index].write(key, value);
    	}
    	for(int i=0; i<numOfReducer; i++) {
    		partitionWriters[i].close();
    	}
		this.reader.close();
	}
   
}