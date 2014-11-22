package lib.input;

import java.io.IOException;

import config.Job;


public class FixedLengthInputFormat extends InputFormat<Long, String> {

	@Override
	public RecordReader<Long, String> getRecordReader(Job job, String path, InputSplit split) throws IOException {
		return new FixedLengthRecordReader(path, split, job.getRecordSize());
	}
	
}
