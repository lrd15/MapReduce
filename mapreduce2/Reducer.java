package mapreduce2;

import java.io.IOException;

import lib.output.RecordWriter;


public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    
public void setup(Context context) { }
	
	public void cleanup(Context context) throws IOException { }
	
	public abstract void reduce(KEYIN key, VALUEIN value, Context context) throws IOException;
	
	public void run(Context context) throws IOException { 
		setup(context);
		while(context.nextKeyValue()) {
			reduce(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	} 
	
	public class Context extends ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		
		public Context(RecordWriter<KEYOUT, VALUEOUT> writer) throws IOException {
			super(writer);
		}
		
		public void write(KEYOUT key, VALUEOUT value) throws IOException {
			super.writer.write(key, value);
		}
	}
}