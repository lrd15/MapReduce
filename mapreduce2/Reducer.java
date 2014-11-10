package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import lib.output.RecordWriter;

public abstract class Reducer<K2, V2, K3, V3> {
    
	private Reducer<K2, V2, K3, V3>.Context context;
	
//	void reduce(K2 key, Iterator<V2> values, OutputCollector<K3,V3> output, Reporter reporter) throws IOException {
//		
//	}
	
	public class Context extends ReduceContext<K2, V2, K3, V3> {
		
		public Context(RecordWriter<K3, V3> writer) throws IOException {
			super(writer);
		}
	}
}