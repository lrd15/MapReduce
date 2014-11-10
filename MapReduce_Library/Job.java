import java.util.ArrayList;
public class Job {
    private Configuration conf;
    private ArrayList<String> inputPaths;
    private String outputPath;
    public Job(Configuration conf) {
        this.conf = conf;
        inputPaths = new ArrayList<String>();
        outputPath = null;
    }

    public void setInputPaths(String... path) {
        for (int i = 0; i < path.length; i++)
            inputPaths.add(path[i]);
    }

    public void addInputPath(String path) {
        inputPaths.add(path);
    }

    public void setOutputPath(String path) {
        outputPath = path;
    }

    public void setMapperClass() {

    }

    public void setReducerClass() {
        
    }
}