package mapreduce2;

import java.io.IOException;

public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    
	public void setup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) { }
	
	public void cleanup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException { 
		context.close();
	}
	
	public abstract void reduce(KEYIN key, Iterable<VALUEIN> values, ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException;
	
	public void run(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException { 
		setup(context);
		System.out.println("Reducer is up...");
		while(context.nextKey()) {
			reduce(context.getCurrentKey(), context.getValues(), context);
		}
		cleanup(context);
	} 
	
}