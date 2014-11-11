package mapreduce2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import lib.input.RecordReader;

public class StringLongIterator implements RawKeyValueIterator<String, Long> {

	private Iterator<RecordReader<String, Long>> readerIterator;

	private String currentKey;
	private ArrayList<String> keys;

	// TODO files of different length
	public StringLongIterator(Iterable<RecordReader<String, Long>> readers)
			throws IOException {
		this.readerIterator = (Iterator<RecordReader<String, Long>>) readers
				.iterator();
		init();
	}

	public String getKey() {
		currentKey = Collections.min(keys);
		return currentKey;
	}

	public Iterable<Long> getValue() throws IOException {
		Iterator<RecordReader<String, Long>> itr = this.readerIterator;
		ArrayList<Long> values = new ArrayList<Long>();
		keys.clear();
		while (itr.hasNext()) {
			RecordReader<String, Long> reader = itr.next();
			if (reader.getCurrentKey() == null)
				continue;
			do {
				String k = reader.getCurrentKey();
				Long v = reader.getCurrentValue();
				if (!k.equals(currentKey)) {
					keys.add(k);
					break;
				}
				values.add(v);
			} while (reader.nextKeyValue());
		}
		return values;
	}

	public boolean next() {
		return !keys.isEmpty();
	}

	private void init() throws IOException {
		Iterator<RecordReader<String, Long>> itr = this.readerIterator;
		while (itr.hasNext()) {
			RecordReader<String, Long> reader = itr.next();
			if (reader.nextKeyValue()) {
				keys.add(reader.getCurrentKey());
			}
		}
	}

}
