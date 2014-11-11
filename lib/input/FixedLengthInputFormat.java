package lib.input;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

import config.Job;


public class FixedLengthInputFormat extends InputFormat<Long, String> {

	static private HashMap<Job, Path> job2Path = new HashMap<Job, Path>();
	
	static public void addInputPaths(Job job, Path path) {
		job2Path.put(job, path);
	}
	
	@Override
	public RecordReader<Long, String> getRecordReader(Job job, InputSplit split) throws IOException {
		return new FixedLengthRecordReader(job2Path.get(job), split, job.getRecordSize());
	}

	@Override
	public InputSplit[] getSplits(Job job, int numSplits) throws IOException {
		InputSplit[] splits = new InputSplit[numSplits];
		Path inputPath = job2Path.get(job);
		long length = inputPath.toFile().length() / numSplits, start = 0;
		System.out.println("splits: " + numSplits);
		System.out.println("length: " + length);
		for(int i=0; i<numSplits; i++) {
			splits[i] = new FileInputSplit(start, length);
			start += length;
		}
		return splits;
	}
}
