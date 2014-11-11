package mapreduce2;

import java.io.IOException;

public interface RawKeyValueIterator<KEYIN, VALUEIN> {
	public KEYIN getKey();
	public Iterable<VALUEIN> getValue() throws IOException;
	public boolean next();
}
