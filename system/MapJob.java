package system;

import lib.input.InputSplit;
public class MapJob {
    int id;
    MapJobSplit[] jobSplits;
    int numCompleted;

    public MapJob(int id, InputSplit[] splits) {
        this.id = id;
        numCompleted = 0;
        int n = splits.length;
        jobSplits = new MapJobSplit[n];
        for (int i = 0; i < n; i++)
            jobSplits[i] = new MapJobSplit(splits[i]);
    }

    public int getID() {
        return id;
    }

    public boolean isCompleted() {
        return numCompleted == jobSplits.length;
    }

    synchronized public void incNumCompleted() {
        numCompleted++;
    }

    public MapJobSplit[] getSplits() {
        return jobSplits;
    }

    public MapJobSplit getSplit(int i) {
        if (i < 0 || i >= jobSplits.length)
            return null;
        return jobSplits[i];
    }
}