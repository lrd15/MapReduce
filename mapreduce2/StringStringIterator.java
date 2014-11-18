package mapreduce2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import lib.input.LineRecordReader;

public class StringStringIterator implements RawKeyValueIterator<String, String> {

	private Iterable<LineRecordReader> readers;

	private String currentKey;
	private ArrayList<String> keyPool;

	public StringStringIterator(Iterable<LineRecordReader> readers) throws IOException {
		this.readers = readers;
		getInitialKeys();
	}

	public String getKey() {
		currentKey = Collections.min(keyPool);
		return currentKey;
	}

	public Iterable<String> getValue() throws IOException {
		ArrayList<String> values = new ArrayList<String>();
		keyPool.clear();
		for(LineRecordReader reader : readers) {
			if (reader.getCurrentKey() == null)
				continue;
			do {
				String k = reader.getCurrentKey();
				String v = reader.getCurrentValue();
				if (!k.equals(currentKey)) {
					keyPool.add(k);
					break;
				}
				values.add(v);
			} while (reader.nextKeyValue());
		}
		return values;
	}

	public boolean next() {
		return !keyPool.isEmpty();
	}

	private void getInitialKeys() throws IOException {
		for(LineRecordReader reader : readers) {
			if (reader.nextKeyValue()) {
				keyPool.add(reader.getCurrentKey());
			}
		}
	}
	
	public void close() throws IOException {
		for(LineRecordReader reader : readers) {
			reader.close();
		}
	}

}
