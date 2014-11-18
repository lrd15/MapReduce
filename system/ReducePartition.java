public class ReducePartition {
    private int[] mapperIds;
    private String[] filenames;
    private JobState state;

    public ReducePartition(int[] ids, String[] files) {
        state = JobState.IDLE;
        mapperIds = new int[ids.length];
        for (int i = 0; i < ids.length; i++)
            mapperIds[i] = ids[i];
        filenames = new String[files.length];
        for (int i = 0; i < files.length; i++)
            filenames[i] = files[i];
    }

    public ReducePartition(ReducePartition p) {
        this(p.getMapperIds(), p.getFilenames());
    }

    public int[] getMapperIds() {
        return mapperIds;
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