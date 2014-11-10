package lib.input;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;


public abstract class InputFormat<Key, Value> {
	
	public abstract RecordReader<Key, Value> getRecordReader(InputSplit split) throws IOException;
	
	public abstract InputSplit[] getSplits(int numSplits) throws IOException;
	
	public static void main(String [] args) throws IOException {
		int recordSize = 10;
		int numSplits = 3;
		Path path = FileSystems.getDefault().getPath("./test/input1");
		FixedLengthInputFormat.setRecordSize(recordSize);
		FixedLengthInputFormat.setInputPath(path);
		FixedLengthInputFormat inputFormat = new FixedLengthInputFormat();
		/*
		InputSplit[] inputSplits = inputFormat.getSplits(numSplits);
		for(InputSplit split : inputSplits) {
			FixedLengthRecordReader recordReader = (FixedLengthRecordReader)inputFormat.getRecordReader(split);
			while(recordReader.nextKeyValue()) {
				Long key = recordReader.getCurrentKey();
				String value = recordReader.getCurrentValue();
				System.out.println(key + " " + value);
			}
		}
		*/
		FileInputSplit split = new FileInputSplit(0l, 30l);
		FixedLengthRecordReader recordReader = new FixedLengthRecordReader(path, split, 10);
		while(recordReader.nextKeyValue()) {
			Long key = recordReader.getCurrentKey();
			String value = recordReader.getCurrentValue();
			System.out.println(key + " " + value);
		}
	}
}
