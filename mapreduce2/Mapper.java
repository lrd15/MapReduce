package mapreduce2;

import java.io.IOException;

import lib.input.*;
import lib.output.*;

public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	public void setup(Context context) { }
	
	public void cleanup(Context context) throws IOException { }
	
	public abstract void map(KEYIN key, VALUEIN value, Context context) throws IOException;
	
	public void run(Context context) throws IOException { 
		setup(context);
		while(context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	} 
	
	public class Context extends MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
		public Context(RecordReader<KEYIN,VALUEIN> reader, 
					   RecordWriter<KEYOUT,VALUEOUT> writer, 
					   InputSplit split) throws IOException {
			super(reader, writer, split);
		}
		
		public void write(KEYOUT key, VALUEOUT value) throws IOException {
			super.writer.write(key, value);
		}
	}
	
}