package lib.input;

import java.io.IOException;
import java.nio.file.Path;


public class FixedLengthInputFormat extends InputFormat<Long, String> {

	static private Path inputPath;
	static private int recordSize;
	
	public FixedLengthInputFormat() { }
	
	static public void setRecordSize(int size) {
		recordSize = size;
	}
	
	static public void setInputPath(Path path) {
		inputPath = path;
	}
	
	@Override
	public RecordReader<Long, String> getRecordReader(InputSplit split) throws IOException {
		return new FixedLengthRecordReader(this.inputPath, split, this.recordSize);
	}

	@Override
	public InputSplit[] getSplits(int numSplits) throws IOException {
		//TODO
		return null;
	}
}
