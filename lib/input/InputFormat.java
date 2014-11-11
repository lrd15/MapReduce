package lib.input;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import config.Job;


public abstract class InputFormat<Key, Value> {
	
	public abstract RecordReader<Key, Value> getRecordReader(Job job, InputSplit split) throws IOException;
	
	public abstract InputSplit[] getSplits(Job job, int numSplits) throws IOException;
	
	public static void main(String [] args) throws IOException {
		int numSplits = 3;
		Job job = Job.getInstance(null, "test input");
		job.setRecordSize(11);
		Path path = FileSystems.getDefault().getPath("./test/input1");
		FixedLengthInputFormat.addInputPaths(job, path);
		FixedLengthInputFormat inputFormat = new FixedLengthInputFormat();
		InputSplit[] inputSplits = inputFormat.getSplits(null, numSplits);
		for(InputSplit split : inputSplits) {
			System.out.println(" -- New Split -- ");
			FixedLengthRecordReader recordReader = 
					(FixedLengthRecordReader)inputFormat.getRecordReader(null, split);
			while(recordReader.nextKeyValue()) {
				Long key = recordReader.getCurrentKey();
				String value = recordReader.getCurrentValue();
				System.out.print(key + " " + value);
			}
		}
	}
}
