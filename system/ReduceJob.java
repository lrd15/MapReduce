public class ReduceJob {
    int id;
    int curPartition;
    int numIdle;
    int numCompleted;
    ReducePartition[] partitions;

    public ReduceJob(int id, ReducePartition[] par) {
        this.id = id;
        curPartition = 0;
        numIdle = par.length;
        numCompleted = 0;
        partitions = new ReducePartition[par.length];
        for (int i =  0; i < par.length; i++)
            partitions[i] = new ReducePartition(par[i]);
    }

    public int getID() {
        return id;
    }

    public boolean isCompleted() {
        return numCompleted == partitions.length;
    }

    public boolean hasNextIdlePartition() {
        return numIdle > 0;
    }

    public void incNumIdle() {
        numIdle++;
    }

    public void decNumIdle() {
        numIdle--;
    }

    synchronized public void incNumCompleted() {
        numCompleted++;
    }

    public int nextIdlePartitionIdx() {
        if (!hasNextIdlePartition())
            return -1;
        while (partitions[curPartition].getState() != JobState.IDLE) {
            curPartition = (curPartition + 1) % partitions.length;
        }
        return curPartition++;
    }

    public ReducePartition getPartition(int i) {
        if (i >= partitions.length)
            return null;
        return partitions[i];
    }
}