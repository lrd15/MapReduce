package lib.input;

import java.io.IOException;
import java.nio.file.Path;

import mapreduce.JobConfig;

public class FixedLengthInputFormat extends InputFormat<Long, String> {

	private int recordSize;
	private Path inputPath;
	
	public FixedLengthInputFormat(int recordSize, Path path) {
		this.recordSize = recordSize;
		this.inputPath = path;
	}
	
	@Override
	public RecordReader<Long, String> getRecordReader(InputSplit split) throws IOException {
		return new FixedLengthRecordReader(split);
	}

	@Override
	public InputSplit[] getSplits(JobConfig job, int numSplits) throws IOException {
		return null;
	}
	
	public int getRecordSize() {
		return this.recordSize;
	}
}
