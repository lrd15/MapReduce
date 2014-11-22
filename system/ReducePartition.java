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
        mapperAddresses = new InetAddress[ips.length];
        for (int i = 0; i < ips.length; i++)
            mapperAddresses[i] = ips[i];
        filenames = new String[files.length];
        for (int i = 0; i < files.length; i++)
            filenames[i] = files[i];
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