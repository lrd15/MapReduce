package mapreduce;

import java.io.IOException;

import lib.input.*;
import lib.output.*;

public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context;
	
	public void setup(Context context) {
		this.context = context;
	}
	
	public void cleanup(Context context) throws IOException { 
		this.context = null;
	}
	
	public abstract void map(KEYIN key, VALUEIN value, Context context) throws IOException;
	
	public void run(Context context) throws IOException { 
		while(context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), this.context);
		}
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