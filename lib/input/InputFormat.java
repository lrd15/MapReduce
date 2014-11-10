package lib.input;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import mapreduce.JobConfig;

public abstract class InputFormat<Key, Value> {
	public abstract RecordReader<Key, Value> getRecordReader(InputSplit split) throws IOException;
	public abstract InputSplit[] getSplits(JobConfig job, int numSplits) throws IOException;
	
	public static void main(String [] args) throws IOException {
		int recordSize = 10;
		Path path = FileSystems.getDefault().getPath("./test/input1");
		FixedLengthInputFormat inputFormat = new FixedLengthInputFormat(recordSize, path);
				
		FixedLengthInputSplit split = new FixedLengthInputSplit(path, 50l, 30l, 10);
		FixedLengthRecordReader recordReader = new FixedLengthRecordReader(split);
		while(recordReader.nextKeyValue()) {
			Long key = recordReader.getCurrentKey();
			String value = recordReader.getCurrentValue();
			System.out.println(key + " " + value);
		}
	}
}
