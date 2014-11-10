package lib.output;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;


public abstract class OutputFormat<Key, Value> {
	public abstract RecordWriter<Key, Value> getRecordWriter() throws IOException;
	
	public static void main(String [] args) throws IOException {
		Path path = FileSystems.getDefault().getPath("./test/output1");
		FileOutputFormat.setOutputPath(path);
		FileOutputFormat outputFormet = new FileOutputFormat();
		RecordWriter recordWriter = outputFormet.getRecordWriter();
		recordWriter.write("test", "value");
		recordWriter.close();
	}
}
