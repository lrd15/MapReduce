package mapreduce2;

import java.io.IOException;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	public void setup(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) { 
		//empty for this project
	}
	
	public void cleanup(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException { 
		context.close();
	}
	
	public void map(KEYIN key, VALUEIN value, MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException {
		context.write((KEYOUT)key, (VALUEOUT)value);
	}
	
	public void run(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException { 
		setup(context);
		while(context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	} 
}