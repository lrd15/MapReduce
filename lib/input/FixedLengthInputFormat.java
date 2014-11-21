package lib.input;

import java.io.File;
import java.io.IOException;

import config.Job;


public class FixedLengthInputFormat extends InputFormat<Long, String> {

	@Override
	public RecordReader<Long, String> getRecordReader(Job job, InputSplit split) throws IOException {
		return new FixedLengthRecordReader(split, job.getRecordSize());
	}

	@Override
	//numSplits - number of splits per file
	public InputSplit[] getSplits(Job job, int numSplits) throws IOException {
		System.out.println("FixedLengthInputFormat getting splits");
		File inputPath = job.getInputPath();
		if(!inputPath.isDirectory()) {
			throw new IOException("The input path doesn't corresponding to any folder");
		}
		File[] files = inputPath.listFiles();
		InputSplit[] splits = new InputSplit[numSplits*files.length];
		for(File file : files) {
			long length = file.length() / numSplits, start = 0;
			System.out.println("Number of splits per file: " + numSplits);
			System.out.println("Number of length per split: " + length);
			for(int i=0; i<numSplits; i++) {
				splits[i] = new FileInputSplit(file, start, length);
				start += length;
			}
		}
		return splits;
	}
}
