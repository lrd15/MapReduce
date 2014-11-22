package lib.input;

import java.io.File;
import java.io.IOException;

import config.Job;


public class FixedLengthInputFormat extends InputFormat<Long, String> {

	private String[] filenames;
	
	@Override
	public RecordReader<Long, String> getRecordReader(Job job, String path, InputSplit split) throws IOException {
		return new FixedLengthRecordReader(path, split, job.getRecordSize());
	}
	
	@Override
	public String[] getFilenames() {
		return this.filenames;
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
		this.filenames = new String[numSplits*files.length];
		for(File file : files) {
			long length = file.length() / numSplits, start = 0;
			System.out.println("Number of splits per file: " + numSplits);
			System.out.println("Number of length per split: " + length);
			for(int i=0; i<numSplits; i++) {
				String filename = file.getName()+i;
				this.filenames[i] = filename;
				splits[i] = new FileInputSplit(filename, start, length);
				//start += length;
			}
		}
		return splits;
	}
}
