package system;

import java.io.Serializable;
import java.net.InetAddress;

public class ReducePartition implements Serializable {
    private InetAddress[] mapperAddresses;
    private String[] filenames;
    private JobState state;
    int workerID;

    public ReducePartition(InetAddress[] ips, String[] files) {
        this(ips, files, -1);
    }

    public ReducePartition(InetAddress[] ips, String[] files, int workerID) {
        this.workerID = workerID;
        state = JobState.IDLE;
        mapperAddresses = ips;
        filenames = files;
    }

    public ReducePartition(ReducePartition p) {
        this(p.getMapperAddresses(), p.getFilenames());
    }

    public InetAddress[] getMapperAddresses() {
        return mapperAddresses;
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