package lib.input;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import config.Job;


public class FixedLengthInputFormat extends InputFormat<Long, String> {

	@Override
	public RecordReader<Long, String> getRecordReader(Job job, InputSplit split) throws IOException {
		return new FixedLengthRecordReader(split, job.getRecordSize());
	}

	@Override
	//numSplits - number of splits per file
	public InputSplit[] getSplits(Job job, int numSplits) throws IOException {
		Path inputPath = job.getInputPath();
		File folder = new File(inputPath.toUri());
		File[] files = folder.listFiles();
		InputSplit[] splits = new InputSplit[numSplits*files.length];
		for(File file : files) {
			long length = file.length() / numSplits, start = 0;
			System.out.println("splits: " + numSplits);
			System.out.println("length: " + length);
			for(int i=0; i<numSplits; i++) {
				splits[i] = new FileInputSplit(Paths.get(file.toURI()), start, length);
				start += length;
			}
		}
		return splits;
	}
}
