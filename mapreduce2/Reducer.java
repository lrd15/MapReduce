package mapreduce2;

import java.io.IOException;

public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    
	public void setup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) { }
	
	public void cleanup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException { 
		context.close();
	}
	
	public void reduce(KEYIN key, Iterable<VALUEIN> values, ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException {
		for(VALUEIN value : values) {
			context.write((KEYOUT)key, (VALUEOUT)value);
		}
	}
	
	public void run(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException { 
		setup(context);
		System.out.print("Reducer working...");
		while(context.nextKey()) {
			reduce(context.getCurrentKey(), context.getValues(), context);
		}
		cleanup(context);
	} 
	
}