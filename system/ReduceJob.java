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
            partitions = new ReducePartition(par);
    }

    public int getId() {
        return id;
    }

    public boolean completed() {
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

    public void incNumCompleted() {
        numCompleted++;
    }

    public void decNumCompleted() {
        numCompleted--;
    }

    public int nextIdlePartitionIdx() {
        if (!hasNextIdlePartition())
            return null;
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