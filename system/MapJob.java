public class MapJob {
    int id;
    MapJobSplit[] jobSplits;

    public MapJob(int id, InputSplit[] splits) {
        this.id = id;
        int n = splits.length;
        jobSplits = new MapJobSplit[n];
        for (int i = 0; i < n; i++)
            jobSplits[i] = new MapJobSplit(splits[i]);
    }

    public int getId() {
        return id;
    }

    public MapJobSplit[] getSplits() {
        return jobSplits;
    }
}