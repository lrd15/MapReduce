package system;

import java.io.Serializable;

public class ReducePartition implements Serializable {
    private int[] mapperIDs;
    private String[] filenames;
    private JobState state;
    int workerID;

    public ReducePartition(int[] ids, String[] files) {
        this(ids, files, -1);
    }

    public ReducePartition(int[] ids, String[] files, int workerID) {
        this.workerID = workerID;
        state = JobState.IDLE;
        mapperIDs = new int[ids.length];
        for (int i = 0; i < ids.length; i++)
            mapperIDs[i] = ids[i];
        filenames = new String[files.length];
        for (int i = 0; i < files.length; i++)
            filenames[i] = files[i];
    }

    public ReducePartition(ReducePartition p) {
        this(p.getMapperIDs(), p.getFilenames());
    }

    public int[] getMapperIDs() {
        return mapperIDs;
    }

    public String[] getFilenames() {
        return filenames;
    }

    public JobState getJobState() {
        return state;
    }

    public void setJobState(JobState s) {
        state = s;
    }

    public int getWorkerID() {
        return workerID;
    }

    public void setWorkerID(int id) {
        workerID = id;
    }
}