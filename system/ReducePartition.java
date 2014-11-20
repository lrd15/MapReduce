public class ReducePartition {
    private int[] mapperIDs;
    private String[] filenames;
    private JobState state;

    public ReducePartition(int[] ids, String[] files) {
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

    public JobState getState() {
        return state;
    }

    public void setState(JobState s) {
        state = s;
    }
}